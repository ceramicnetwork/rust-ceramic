use crate::sql;
use crate::Network;
use anyhow::{Error, Result};
use ceramic_core::{EventId, StreamId};
use ceramic_p2p::SQLiteBlockStore;
use chrono::{SecondsFormat, Utc};
use cid::{
    multibase,
    multihash,
    multihash::{Code, MultihashDigest},
    Cid,
};
use clap::{Args, Subcommand};
use futures_util::StreamExt;
use glob::{glob, Paths};
use minicbor::{data::Tag, data::Type, display, Decode, Decoder};
use ordered_float::OrderedFloat;
use sqlx::sqlite::SqlitePool;
use std::collections::{btree_map::BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::fs;
use tracing::debug;

#[derive(Subcommand, Debug)]
pub enum EventsCommand {
    /// Slurp events into the local event database.
    Slurp(SlurpOpts),
    ///
    ScanBlockstore(ScanBlockstoreOpts),
}

#[derive(Args, Debug)]
pub struct SlurpOpts {
    /// The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    #[clap(long, short, value_parser)]
    input_ipfs_path: Option<PathBuf>,

    /// The path to the input_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    #[clap(long, short = 'c', value_parser)]
    input_ceramic_db: Option<PathBuf>,

    /// The path to the output_ceramic_db [eg: ~/.ceramic-one/db.sqlite3]
    #[clap(long, short, value_parser)]
    output_ceramic_path: Option<PathBuf>,
}

pub async fn events(cmd: EventsCommand) -> Result<()> {
    match cmd {
        EventsCommand::Slurp(opts) => slurp(opts).await,
        EventsCommand::ScanBlockstore(opts) => scan_blockstore1(opts).await,
    }
}

async fn slurp(opts: SlurpOpts) -> Result<()> {
    let home: PathBuf = dirs::home_dir().unwrap_or("/data/".into());
    let default_output_ceramic_path: PathBuf = home.join(".ceramic-one/db.sqlite3");

    let output_ceramic_path = opts
        .output_ceramic_path
        .unwrap_or(default_output_ceramic_path);
    println!(
        "{} Opening output ceramic SQLite DB at: {}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        output_ceramic_path.display()
    );

    let pool: sqlx::Pool<sqlx::Sqlite> = SqlitePool::connect(&format!(
        "sqlite:{}?mode=rwc",
        output_ceramic_path
            .to_str()
            .expect("path should be utf8 compatible")
    ))
    .await
    .unwrap();
    let store = SQLiteBlockStore::new(pool).await.unwrap();

    if let Some(input_ceramic_db) = opts.input_ceramic_db {
        migrate_from_database(input_ceramic_db, store.clone()).await;
    }
    if let Some(input_ipfs_path) = opts.input_ipfs_path {
        migrate_from_filesystem(input_ipfs_path, store.clone()).await;
    }
    Ok(())
}

async fn migrate_from_filesystem(input_ipfs_path: PathBuf, store: SQLiteBlockStore) {
    // the block store is split in to 1024 directories and then the blocks stored as files.
    // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
    // The leading "B" for the b32 sha256 multihash is left off
    // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
    let p = input_ipfs_path
        .join("**/*")
        .to_str()
        .expect("expect utf8")
        .to_owned();
    println!(
        "{} Opening IPFS Repo at: {}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        &p
    );
    let paths: Paths = glob(&p).unwrap();

    let mut count = 0;
    let mut err_count = 0;

    for path in paths {
        let path = path.unwrap().as_path().to_owned();
        if !path.is_file() {
            continue;
        }

        let Ok((_base, hash_bytes)) =
            multibase::decode("B".to_string() + path.file_stem().unwrap().to_str().unwrap())
        else {
            println!(
                "{} {:?} is not a base32upper multihash.",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display()
            );
            err_count += 1;
            continue;
        };
        let Ok(hash) = multihash::Multihash::from_bytes(&hash_bytes) else {
            println!(
                "{} {:?} is not a base32upper multihash.",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display()
            );
            err_count += 1;
            continue;
        };
        let cid = Cid::new_v1(0x71, hash);
        let blob = fs::read(&path).unwrap();

        if count % 10000 == 0 {
            println!(
                "{} {} {} ok:{}, err:{}",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display(),
                cid,
                count,
                err_count
            );
        }

        let result = store.put(cid, blob.into(), vec![]).await;
        if result.is_err() {
            println!(
                "{} err: {} {:?}",
                Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                path.display(),
                result
            );
            err_count += 1;
            continue;
        }
        count += 1;
    }

    println!(
        "{} count={}, err_count={}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        count,
        err_count
    );
}

async fn migrate_from_database(input_ceramic_db: PathBuf, store: SQLiteBlockStore) {
    let input_ceramic_db_filename = input_ceramic_db.to_str().expect("expect utf8");
    println!(
        "{} Importing blocks from {}.",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        input_ceramic_db_filename
    );
    let result = store.merge_from_sqlite(input_ceramic_db_filename).await;
    println!(
        "{} Done importing blocks from {}.",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        input_ceramic_db_filename
    );
    result.unwrap()
}

#[derive(Args, Debug)]
pub struct ScanBlockstoreOpts {
    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: Network,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,
}

#[derive(Decode)]
#[cbor(map)]
struct COSE {
    // JOSE: JSON Object Signing and Encryption
    // COSE: CBOR Object Signing and Encryption
    //
    // {"payload": h'',
    //  "signatures": [{"protected": h'',
    //                  "signature": h''}]}
    #[n(0)]
    #[cbor(encode_with = "minicbor::bytes::encode")]
    #[cbor(decode_with = "minicbor::bytes::decode")]
    pub payload: Vec<u8>,
    #[n(1)]
    pub signatures: Vec<Signatures>,
}

#[derive(Decode)]
struct Signatures {
    // [ {"protected": h'', "signature": h''} ]
    #[n(0)]
    pub payload: Vec<u8>,
    #[n(1)]
    pub protected: Vec<u8>,
    #[n(2)]
    pub signature: Vec<u8>,
}

pub async fn scan_blockstore1(opts: ScanBlockstoreOpts) -> Result<()> {
    let dir = match opts.store_dir.clone() {
        Some(dir) => dir,
        None => match home::home_dir() {
            Some(home_dir) => home_dir.join(".ceramic-one"),
            None => PathBuf::from(".ceramic-one"),
        },
    };
    debug!("using directory: {}", dir.display());
    println!("using directory: {}", dir.display());
    let sql_db_path: PathBuf = dir.join("db.sqlite3");
    let sql_pool: sqlx::Pool<sqlx::Sqlite> = sql::connect(&sql_db_path).await?;
    let sql_store: SQLiteBlockStore = SQLiteBlockStore::new(sql_pool).await.unwrap();
    scan_blockstore2(opts, sql_store).await
}

// scan the blockstore and add found events to the recon table.
async fn scan_blockstore2(opts: ScanBlockstoreOpts, sql_store: SQLiteBlockStore) -> Result<()> {
    // What are we looking for?
    //   StreamIDs
    //   signed envelopes
    //   header.controllers[0] DIDs
    //   id CIDs
    //
    //   for each Event we want to find all the blocks build that event
    //   for init events this may be just the Event Block
    //   for signed events this will include the envelope
    //   for time events this will include the proof and Merkle tree path
    //   the EventID and the set of block CIDs should be stored so we can send a CAR for that event on request.
    //
    // dag-cbor
    // {"header": {"model": "k_model_id"
    //             "controllers": ["did:method:method-specific-id"]}}
    //
    // dag-jose
    // {"payload": CID,
    //  "signatures": [{"protected": b'{"alg":"EdDSA","cap":"ipfs://CID","kid":"did:key:*"}',
    //                  "signature": SIG}]}
    //
    // if not cbor continue
    // if not a map continue
    // if map lacks header key continue
    // if header is not map continue
    // if header lacks controllers continue
    // if controllers is not array continue
    // if header lacks model continue
    // if prev in header follow prev back to init to find hight
    // build eventID.new(
    //   network: &Network, // e.g. Network::Mainnet
    //   sort_key: &str,    // e.g. "model"
    //   sort_value: &str,  // e.g. "kh4q0ozorrgaq2mezktnrmdwleo1d" // cspell:disable-line
    //   controller: &str,  // e.g. "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9"
    //   init: &Cid, // e.g. Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq") // cspell:disable-line
    //   event_height: u64, // e.g. 1
    //   event_cid: &Cid, // e.g. Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy") // cspell:disable-line
    // )

    let network = &opts.network.to_network(&opts.local_network_id)?;

    let mut found_stream_ids = BTreeSet::<String>::new();
    let mut found_dids = BTreeMap::<String, u64>::new();
    scan_for_stream_ids(&mut found_stream_ids, &sql_store).await?;

    let mut count = 0;
    let mut stream_id_parsed_count = 0;
    let mut init_block_found_count = 0;
    let mut envelope_payload_block_found_count = 0;
    let mut envelope_payload_block_parsed_count = 0;
    let mut header_count = 0;
    let mut controllers_count = 0;
    let mut model_count = 0;

    let mut found_count = 0;
    let mut not_found_count = 0;
    let mut error_count = 0;
    let mut stream_ids_parse_fail = 0;
    let count_stream_id = found_stream_ids.len();
    // scan stream_ids
    for stream_id in found_stream_ids {
        count += 1;
        let Ok(stream_id) = StreamId::from_str(&stream_id) else {
            continue;
        };
        stream_id_parsed_count += 1;

        // if the StreamID parses try to get it from the blockstore
        let block = sql_store.get(stream_id.cid).await;
        let Ok(block) = block else {
            // sql error
            error_count += 1;
            continue;
        };
        let Some(bytes) = block else {
            // CID not found in block store
            not_found_count += 1;
            continue;
        };
        init_block_found_count += 1;

        if count % 100_000 == 0 {
            println!(
                "{}/{}: ({}, {}, {}), {}, {}, {:?}, found_DIDs={}",
                count,
                count_stream_id,
                found_count,
                error_count,
                not_found_count,
                stream_id,
                stream_id.cid,
                bytes,
                found_dids.len(),
            );
        }

        let Ok(CborValue::Map(cbor)) = CborValue::parse(bytes.as_ref()) else {
            continue;
        };

        let Some(CborValue::Bytes(cid_bytes)) = cbor.get(&"payload".into()) else {
            continue;
        };
        let Ok(init_cid) = Cid::from_str(&("f".to_owned() + hex::encode(cid_bytes).as_str()))
        else {
            continue;
        };
        let Ok(Some(payload_bytes)) = sql_store.get(init_cid).await else {
            continue;
        };
        envelope_payload_block_found_count += 1;

        let Ok(CborValue::Map(cbor)) = CborValue::parse(payload_bytes.as_ref()) else {
            continue;
        };
        envelope_payload_block_parsed_count += 1;

        let Some(CborValue::Map(header)) = cbor.get(&"header".into()) else {
            continue;
        };
        header_count += 1;

        let Some(CborValue::Array(controllers)) = header.get(&"controllers".into()) else {
            continue;
        };
        controllers_count += 1;

        let mut first_controller = None;
        for controller in controllers.iter() {
            let CborValue::String(controller) = controller else {
                continue;
            };
            if first_controller.is_none() {
                first_controller = Some(controller);
            }
            let count = found_dids.entry(controller.to_string()).or_default();
            *count += 1;
        }
        let Some(CborValue::String(sort_value)) = header.get(&"model".into()) else {
            continue;
        };
        model_count += 1;

        let event_height = 0; // remove me
        let event_id = EventId::new(
            network,                   // e.g. Network::Mainnet
            "model",                   // e.g. "model"
            sort_value, // e.g. "kh4q0ozorrgaq2mezktnrmdwleo1d" // cspell:disable-line
            first_controller.unwrap(), // e.g. "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9"
            &init_cid, // e.g. Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq") // cspell:disable-line
            event_height, // e.g. 1
            &init_cid,
        );
        println!("event_id={event_id}");
    }
    println!(
        "{}/{}: ({}, {}, {}), \
        stream_ids_parse_fail={}, \
        stream_id_parsed_count={}, \
        init_block_found_count,={}, \
        envelope_payload_block_found_count={}, \
        envelope_payload_block_parsed_count={}, \
        header_count={}, \
        controllers_count={}, \
        model_count={}",
        count,
        count_stream_id,
        found_count,
        not_found_count,
        error_count,
        stream_ids_parse_fail,
        stream_id_parsed_count,
        init_block_found_count,
        envelope_payload_block_found_count,
        envelope_payload_block_parsed_count,
        header_count,
        controllers_count,
        model_count,
    );
    Ok(())
}

async fn scan_for_stream_ids(
    found_stream_ids: &mut BTreeSet<String>,
    sql_store: &SQLiteBlockStore,
) -> Result<()> {
    // Scan all blocks for StreamIDs
    let mut count = 0;
    let mut count_not_cbor = 0;
    let mut count_not_map = 0;
    let mut count_map = 0;
    let mut key_sets = BTreeMap::<String, u64>::new();
    let mut rows = sql_store.scan();
    while let Some(row) = rows.next().await {
        count += 1;
        let Ok(row) = row else {
            continue;
        };
        let Ok(cbor): Result<CborValue, Error> = CborValue::parse(row.bytes.as_slice()) else {
            count_not_cbor += 1;
            continue;
        };

        let map = match cbor {
            CborValue::Map(m) => m,
            _ => {
                count_not_map += 1;
                continue;
            }
        };
        count_map += 1;

        if let Some(CborValue::Array(stream_ids)) = &map.get(&"streamIds".into()) {
            for stream_id in stream_ids.iter() {
                if let CborValue::String(stream_id) = stream_id {
                    found_stream_ids.insert(stream_id.to_owned());
                }
            }
        }

        let keys_string = map
            .keys()
            .map(|key| match key {
                CborValue::String(s) => s.to_owned(),
                _ => todo!(),
            })
            .collect::<Vec<String>>()
            .join(",");

        key_sets.insert(
            keys_string.clone(),
            key_sets.get(&keys_string).unwrap_or(&0) + 1,
        );

        if key_sets.get(&keys_string) == Some(&1) {
            println!(
                "{} display: {} {}",
                count,
                Cid::new_v1(0x71, Code::Sha2_256.digest(&row.bytes)),
                display(&row.bytes)
            );
        }

        if count % 1_000_000 == 0 {
            println!(
                "{}: count_not_cbor={} count_not_map={}, count_map={},  key_sets={}, count_stream_id={}",
                count,
                count_not_cbor,
                count_not_map,
                count_map,
                key_sets.len(),
                found_stream_ids.len(),
            );
        }
    }
    println!(
        "{}: count_map={}, count_not_map={}, key_sets={:?}, count_stream_id={}",
        count,
        count_map,
        count_not_map,
        key_sets,
        found_stream_ids.len(),
    );
    Ok(())
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub enum CborValue {
    Bool(bool),
    Null,
    Undefined,
    U64(u64),
    I64(i64),
    Int(i128), // Cbor ints range from [ -(2^64) , 2^64-1 ] to fit this i65 we use i128
    F64(OrderedFloat<f64>), // this makes it possible to put CborValue in BTreeMap
    Simple(u8),
    Bytes(Vec<u8>),
    BytesIndef(Vec<u8>),
    String(String),
    StringIndef(String),
    Array(Vec<CborValue>),
    ArrayIndef(Vec<CborValue>),
    Map(BTreeMap<CborValue, CborValue>),
    MapIndef(BTreeMap<CborValue, CborValue>),
    Tag((Tag, Box<CborValue>)),
    Break, // 0xff
    Unknown(u8),
}

impl CborValue {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        CborValue::next(&mut decoder)
    }
    fn next(decoder: &mut Decoder) -> Result<CborValue> {
        match decoder.datatype() {
            Ok(Type::Bool) => Ok(CborValue::Bool(decoder.bool()?)),
            Ok(Type::Null) => {
                decoder.null()?;
                Ok(CborValue::Null)
            }
            Ok(Type::Undefined) => {
                decoder.undefined()?;
                Ok(CborValue::Undefined)
            }
            Ok(Type::U8) => Ok(CborValue::U64(decoder.u8()? as u64)),
            Ok(Type::U16) => Ok(CborValue::U64(decoder.u16()? as u64)),
            Ok(Type::U32) => Ok(CborValue::U64(decoder.u32()? as u64)),
            Ok(Type::U64) => Ok(CborValue::U64(decoder.u64()?)),
            Ok(Type::I8) => Ok(CborValue::I64(decoder.i8()? as i64)),
            Ok(Type::I16) => Ok(CborValue::I64(decoder.i16()? as i64)),
            Ok(Type::I32) => Ok(CborValue::I64(decoder.i32()? as i64)),
            Ok(Type::I64) => Ok(CborValue::I64(decoder.i64()?)),
            Ok(Type::Int) => todo!(),
            Ok(Type::F16) => Ok(CborValue::F64(OrderedFloat(decoder.f16()?.into()))),
            Ok(Type::F32) => Ok(CborValue::F64(OrderedFloat(decoder.f32()?.into()))),
            Ok(Type::F64) => Ok(CborValue::F64(OrderedFloat(decoder.f64()?))),
            Ok(Type::Simple) => todo!(),
            Ok(Type::Bytes) => Ok(CborValue::Bytes(decoder.bytes()?.to_vec())),
            Ok(Type::BytesIndef) => todo!(),
            Ok(Type::String) => Ok(CborValue::String(decoder.str()?.to_string())),
            Ok(Type::StringIndef) => todo!(),
            Ok(Type::Array) => {
                let mut array = Vec::new();
                for _ in 0..decoder.array().unwrap().unwrap() {
                    let value = CborValue::next(decoder)?;
                    array.push(value);
                }
                Ok(CborValue::Array(array))
            }
            Ok(Type::ArrayIndef) => todo!(),
            Ok(Type::Map) => {
                let mut map = BTreeMap::new();
                for _ in 0..decoder.map().unwrap().unwrap() {
                    let key = CborValue::next(decoder)?;
                    let value = CborValue::next(decoder)?;
                    map.insert(key, value);
                }
                Ok(CborValue::Map(map))
            }
            Ok(Type::MapIndef) => todo!(),
            Ok(Type::Tag) => {
                let tag = decoder.tag()?;
                let value = CborValue::next(decoder)?;
                Ok(CborValue::Tag((tag, Box::new(value))))
            }
            Ok(Type::Break) => todo!(),
            Ok(Type::Unknown(_)) => todo!(),
            Err(_) => todo!(),
        }
    }
}

impl From<&[u8]> for CborValue {
    fn from(bytes: &[u8]) -> Self {
        CborValue::Bytes(bytes.to_vec())
    }
}

impl From<&str> for CborValue {
    fn from(string: &str) -> Self {
        CborValue::String(string.to_string())
    }
}
