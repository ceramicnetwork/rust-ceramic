use anyhow::{anyhow, Error, Result};
use ceramic_p2p::SQLiteBlockStore;
use ceramic_store::SqlitePool;
use chrono::{SecondsFormat, TimeZone, Utc};
use cid::{multibase, multihash, Cid};
use clap::{Args, Subcommand};
use enum_as_inner::EnumAsInner;
use glob::{glob, Paths};
use minicbor::{data::Tag, data::Type, Decoder};
use multihash::Multihash;
use ordered_float::OrderedFloat;
use sqlx::Row;
use std::{collections::BTreeMap, fs, ops::Index, path::PathBuf, str::FromStr};
use tracing::debug;

#[derive(Subcommand, Debug)]
pub enum EventsCommand {
    /// Slurp events into the local event database.
    Slurp(SlurpOpts),
    Validate(ValidateOpts),
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

#[derive(Args, Debug)]
pub struct ValidateOpts {
    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,

    /// CID of the block to validate
    #[arg(short, long)]
    cid: Cid,

    /// Ethereum RPC URL, e.g. ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/<api_key>
    #[arg(short, long, env = "ETHEREUM_RPC_URL")]
    ethereum_rpc_url: String,
}

pub async fn events(cmd: EventsCommand) -> Result<()> {
    match cmd {
        EventsCommand::Slurp(opts) => slurp(opts).await,
        EventsCommand::Validate(opts) => validate(opts).await,
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

    let pool = SqlitePool::connect(output_ceramic_path).await.unwrap();
    let block_store = SQLiteBlockStore::new(pool).await.unwrap();

    if let Some(input_ceramic_db) = opts.input_ceramic_db {
        migrate_from_database(input_ceramic_db, block_store.clone()).await;
    }
    if let Some(input_ipfs_path) = opts.input_ipfs_path {
        migrate_from_filesystem(input_ipfs_path, block_store.clone()).await;
    }
    Ok(())
}

async fn validate(opts: ValidateOpts) -> Result<()> {
    let home: PathBuf = dirs::home_dir().unwrap_or("/data/".into());
    let store_dir = opts
        .store_dir
        .unwrap_or(home.join(".ceramic-one/db.sqlite3"));
    println!(
        "{} Opening ceramic SQLite DB at: {}",
        Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        store_dir.display()
    );

    let pool = SqlitePool::connect(store_dir.join("db.sqlite3"))
        .await
        .unwrap();
    let block_store = SQLiteBlockStore::new(pool.clone()).await.unwrap();
    let root_store = SQLiteRootStore::new(pool).await.unwrap();

    // Validate that the CID is either DAG-CBOR or DAG-JOSE
    if (opts.cid.codec() != 0x71) && (opts.cid.codec() != 0x85) {
        return Err(anyhow!("CID {} is not a valid Ceramic event", opts.cid));
    }

    // If the CID is a DAG-JOSE, the event is either a signed Data Event or a signed Init Event, both of which can be
    // validated similarly.
    if opts.cid.codec() == 0x85 {
        let controller = validate_data_event_envelope().await;
        match controller {
            Ok(controller) => {
                println!(
                    "DataEvent({}) validated as authored by Controller({})",
                    opts.cid, controller
                )
            }
            Err(e) => println!("{} failed with error: {:?}", opts.cid, e),
        }
        return Ok(());
    }
    // If the CID is a DAG-CBOR, the event is either a Time Event or an unsigned Init Event. In this case, we'll pull
    // the block from the store and use the presence of the "proof" field to determine whether this is a Time Event or
    // an unsigned Init Event.
    //
    // When validating events from Recon, the block store will be a CARFileBlockStore that wraps the CAR file received
    // over Recon.
    if let Some(block) = block_store.get(opts.cid).await? {
        let event = CborValue::parse(&block)?;
        if event.get_key("proof").is_some() {
            let timestamp = validate_time_event(
                &opts.cid,
                &block_store,
                &root_store,
                opts.ethereum_rpc_url.as_str(),
            )
            .await;
            match timestamp {
                Ok(timestamp) => {
                    let rfc3339 = Utc
                        .timestamp_opt(timestamp, 0)
                        .unwrap()
                        .to_rfc3339_opts(SecondsFormat::Secs, true);
                    println!(
                        "TimeEvent({}) validated at Timestamp({}) = {}",
                        opts.cid, timestamp, rfc3339
                    )
                }
                Err(e) => println!("{} failed with error: {:?}", opts.cid, e),
            }
        } else {
            if event.get_key("data").is_some() {
                println!("UnsignedDataEvent({}) is not signed", opts.cid);
            }
            let controller = validate_data_event_payload().await;
            match controller {
                Ok(controller) => {
                    println!(
                        "UnsignedEvent({}) validated as authored by Controller({})",
                        opts.cid, controller
                    )
                }
                Err(e) => println!("{} failed with error: {:?}", opts.cid, e),
            }
        }
    }
    Ok(())
}

async fn validate_data_event_envelope() -> Result<String> {
    todo!("implement me")
}

async fn validate_data_event_payload() -> Result<String> {
    todo!("implement me")
}

// To validate a Time Event, we need to prove:
// - TimeEvent/prev == TimeEvent/proof/root/${TimeEvent/path}
// - TimeEvent/proof/root is in the Root Store
//
// - If root not in local root store try to read it from txHash.
// - Validated time is the time from the Root Store
async fn validate_time_event(
    cid: &Cid,
    block_store: &SQLiteBlockStore,
    root_store: &SQLiteRootStore,
    ethereum_rpc_url: &str,
) -> Result<i64> {
    if let Some(block) = block_store.get(cid.to_owned()).await? {
        let time_event = CborValue::parse(&block)?;
        // Destructure the proof to get the tag and the value
        let proof_cid: Cid = time_event.path(&["proof"]).try_into()?;
        let prev: Cid = time_event.path(&["prev"]).try_into()?;
        if let Some(proof_block) = block_store.get(proof_cid).await? {
            let proof_cbor = CborValue::parse(&proof_block)?;
            let proof_root: Cid = proof_cbor.path(&["root"]).try_into()?;
            let path: String = time_event.path(&["path"]).try_into()?;

            // If prev not in root then TimeEvent is not valid.
            if !prev_in_root(prev, proof_root, path, block_store).await? {
                return Err(anyhow!("prev {} not in root {}", prev, proof_root));
            }

            // if root in root_store return timestamp.
            // note: at some point we will need a negative cache to exponentially backoff eth_getTransactionByHash
            if let Ok(Some(timestamp)) = root_store.get(proof_root.hash().digest()).await {
                return Ok(timestamp);
            }

            // else eth_transaction_by_hash
            let tx_hash_cid: Cid = proof_cbor.path(&["txHash"]).try_into()?;
            let (transaction_root, timestamp) =
                eth_transaction_by_hash(tx_hash_cid, ethereum_rpc_url).await?;
            debug!("root: {}, timestamp: {}", transaction_root, timestamp);

            if transaction_root == proof_root {
                root_store
                    .put(transaction_root.hash().digest(), timestamp)
                    .await?;
                Ok(timestamp)
            } else {
                Err(anyhow!(
                    "root from transaction {} != root from proof {}",
                    transaction_root,
                    proof_root
                ))
            }
        } else {
            Err(anyhow!("proof {} not found", cid))
        }
    } else {
        Err(anyhow!("CID {} not found", cid))
    }
}

async fn prev_in_root(
    prev: Cid,
    root: Cid,
    path: String,
    block_store: &SQLiteBlockStore,
) -> Result<bool> {
    let mut current_cid = root;
    for segment in path.split('/') {
        let Some(block) = block_store.get(current_cid).await? else {
            return Err(anyhow!(
                "CID {} not found in prev_in_root test",
                current_cid
            ));
        };
        let current = CborValue::parse(&block)?;
        current_cid = current.as_array().unwrap()[usize::from_str(segment)?]
            .clone()
            .try_into()?;
    }
    Ok(prev == current_cid)
}

async fn eth_transaction_by_hash(cid: Cid, ethereum_rpc_url: &str) -> Result<(Cid, i64)> {
    // curl https://mainnet.infura.io/v3/{api_token} \
    //   -X POST \
    //   -H "Content-Type: application/json" \
    //   -d '{"jsonrpc":"2.0","method":"eth_getTransactionByHash", "params":["0x{tx_hash}"],"id":1}'
    let tx_hash = format!("0x{}", hex::encode(cid.hash().digest()));
    let client = reqwest::Client::new();
    let res = client
        .post(ethereum_rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1,
        }))
        .send()
        .await?;
    let json: serde_json::Value = res.json().await?;
    debug!("txByHash response: {}", json);
    if let Some(result) = json.get("result") {
        if let Some(block_hash) = result.get("blockHash") {
            if let Some(block_hash) = block_hash.as_str() {
                if let Some(input) = result.get("input") {
                    if let Some(input) = input.as_str() {
                        return Ok((
                            get_root_from_input(input)?,
                            eth_block_by_hash(block_hash, ethereum_rpc_url).await?,
                        ));
                    }
                }
            }
        }
    }
    Err(anyhow!("missing fields"))
}

async fn eth_block_by_hash(block_hash: &str, ethereum_rpc_url: &str) -> Result<i64> {
    // curl https://mainnet.infura.io/v3/{api_token} \
    //     -X POST \
    //     -H "Content-Type: application/json" \
    //     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
    let client = reqwest::Client::new();
    let res = client
        .post(ethereum_rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByHash",
            "params": [block_hash, false],
            "id": 1,
        }))
        .send()
        .await?;
    let json: serde_json::Value = res.json().await?;
    debug!("blockByHash response: {}", json);
    if let Some(timestamp) = json["result"]["timestamp"].as_str() {
        get_timestamp_from_hex_string(timestamp)
    } else {
        Err(anyhow!("missing fields"))
    }
}

fn get_root_from_input(input: &str) -> Result<Cid> {
    if let Some(input) = input.strip_prefix("0x97ad09eb") {
        // Strip "0x97ad09eb" from the input and convert it into a cidv1 - dag-cbor - (sha2-256 : 256)
        // 0x12 -> sha2-256
        // 0x20 -> 256 bits of hash
        let root_bytes = [vec![0x12_u8, 0x20], hex::decode(input)?.to_vec()].concat();
        Ok(Cid::new_v1(0x71, Multihash::from_bytes(&root_bytes)?))
    } else {
        Err(anyhow!("input is not anchor-cbor"))
    }
}

fn get_timestamp_from_hex_string(ts: &str) -> Result<i64> {
    // Strip "0x" from the timestamp and convert it to a u64
    if let Some(ts) = ts.strip_prefix("0x") {
        Ok(i64::from_str_radix(ts, 16)?)
    } else {
        Err(anyhow!("timestamp is not valid hex"))
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug, EnumAsInner)]
pub enum CborValue {
    // not clear if Indef should be treated distinctly from the finite versions
    Bool(bool),
    Null,
    Undefined,
    U64(u64),
    I64(i64),
    Int(i128), // Cbor ints range from [ -(2^64) , 2^64-1 ] to fit this i65 we use i128
    F64(OrderedFloat<f64>), // this makes it possible to put CborValue in BTreeMap
    Simple(u8),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<CborValue>),
    Map(BTreeMap<CborValue, CborValue>),
    Tag((Tag, Box<CborValue>)),
    Break, // 0xff
    Unknown(u8),
}

impl CborValue {
    fn parse(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        let c = CborValue::next(&mut decoder);
        if decoder.position() != bytes.len() {
            debug!("decoder not at end {}/{}", decoder.position(), bytes.len());
            return Err(anyhow!("CborValue decode error not at end"));
        }
        c
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
            Ok(Type::U8) => Ok(CborValue::U64(decoder.u8()?.into())),
            Ok(Type::U16) => Ok(CborValue::U64(decoder.u16()?.into())),
            Ok(Type::U32) => Ok(CborValue::U64(decoder.u32()?.into())),
            Ok(Type::U64) => Ok(CborValue::U64(decoder.u64()?)),
            Ok(Type::I8) => Ok(CborValue::I64(decoder.i8()?.into())),
            Ok(Type::I16) => Ok(CborValue::I64(decoder.i16()?.into())),
            Ok(Type::I32) => Ok(CborValue::I64(decoder.i32()?.into())),
            Ok(Type::I64) => Ok(CborValue::I64(decoder.i64()?)),
            Ok(Type::Int) => Ok(CborValue::Int(decoder.int()?.into())),
            Ok(Type::F16) => Ok(CborValue::F64(OrderedFloat(decoder.f16()?.into()))),
            Ok(Type::F32) => Ok(CborValue::F64(OrderedFloat(decoder.f32()?.into()))),
            Ok(Type::F64) => Ok(CborValue::F64(OrderedFloat(decoder.f64()?))),
            Ok(Type::Simple) => Ok(CborValue::Simple(decoder.simple()?)),
            Ok(Type::Bytes) => Ok(CborValue::Bytes(decoder.bytes()?.to_vec())),
            Ok(Type::BytesIndef) => Ok(CborValue::Undefined), // TODO: support Type::BytesIndef
            Ok(Type::String) => Ok(CborValue::String(decoder.str()?.to_string())),
            Ok(Type::StringIndef) => Ok(CborValue::Undefined), // TODO: support Type::StringIndef
            Ok(Type::Array) => {
                let mut array = Vec::new();
                for _ in 0..decoder.array().unwrap().unwrap() {
                    let value = CborValue::next(decoder)?;
                    array.push(value);
                }
                Ok(CborValue::Array(array))
            }
            Ok(Type::ArrayIndef) => {
                let mut array = Vec::new();
                loop {
                    let value = CborValue::next(decoder)?;
                    if let CborValue::Break = value {
                        break;
                    }
                    array.push(value)
                }
                Ok(CborValue::Array(array))
            }
            Ok(Type::Map) => {
                let mut map = BTreeMap::new();
                for _ in 0..decoder.map().unwrap().unwrap() {
                    let key = CborValue::next(decoder)?;
                    let value = CborValue::next(decoder)?;
                    map.insert(key, value);
                }
                Ok(CborValue::Map(map))
            }
            Ok(Type::MapIndef) => Ok(CborValue::Undefined), // TODO: support Type::MapIndef
            Ok(Type::Tag) => {
                let tag: Tag = decoder.tag()?;
                let value = CborValue::next(decoder)?;
                Ok(CborValue::Tag((tag, Box::new(value))))
            }
            Ok(Type::Break) => Ok(CborValue::Break),
            Ok(Type::Unknown(additional)) => Ok(CborValue::Unknown(additional)),
            Err(e) => Err(e.into()),
        }
    }

    pub fn path(&self, parts: &[&str]) -> CborValue {
        match parts.split_first() {
            None => self.clone(), // if there are no parts this is thing at the path.
            Some((first, rest)) => {
                let CborValue::Map(map) = &self else {
                    return CborValue::Undefined; // if self is not a map there is no thing at the path.
                };
                let Some(next) = map.get(&first.to_string().into()) else {
                    return CborValue::Undefined; // if the next part is not in the map there is no thing at the path.
                };
                next.path(rest)
            }
        }
    }

    pub fn untag(&self, tag: u64) -> Option<&CborValue> {
        if let CborValue::Tag((Tag::Unassigned(inner_tag), boxed_value)) = &self {
            if inner_tag == &tag {
                return Some(boxed_value);
            }
        }
        None
    }

    pub fn type_name(&self) -> String {
        match self {
            CborValue::Bool(_) => "bool".to_string(),
            CborValue::Null => "null".to_string(),
            CborValue::Undefined => "undefined".to_string(),
            CborValue::U64(_) => "u64".to_string(),
            CborValue::I64(_) => "i64".to_string(),
            CborValue::Int(_) => "int".to_string(),
            CborValue::F64(_) => "f64".to_string(),
            CborValue::Simple(_) => "simple".to_string(),
            CborValue::Bytes(_) => "bytes".to_string(),
            CborValue::String(_) => "string".to_string(),
            CborValue::Array(_) => "array".to_string(),
            CborValue::Map(_) => "map".to_string(),
            CborValue::Tag(_) => "tag".to_string(),
            CborValue::Break => "break".to_string(),
            CborValue::Unknown(_) => "unknown".to_string(),
        }
    }

    // pub fn get_index(&self, index: usize) -> Option<&CborValue> {
    //     self.as_array()?.get(index)
    // }

    pub fn get_key(&self, key: &str) -> Option<&CborValue> {
        self.as_map()?.get(&CborValue::String(key.to_owned()))
    }
}

impl From<&[u8]> for CborValue {
    fn from(bytes: &[u8]) -> Self {
        CborValue::Bytes(bytes.to_vec())
    }
}

impl TryInto<Vec<u8>> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            CborValue::Bytes(bytes) => Ok(bytes.to_vec()),
            CborValue::String(string) => Ok(string.as_bytes().to_vec()),
            _ => Err(anyhow!("{} not Bytes or String", self.type_name())),
        }
    }
}

impl From<&str> for CborValue {
    fn from(string: &str) -> Self {
        CborValue::String(string.to_string())
    }
}

impl From<String> for CborValue {
    fn from(string: String) -> Self {
        CborValue::String(string)
    }
}

impl TryInto<Cid> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<Cid, Self::Error> {
        if let Some(CborValue::Bytes(cid_bytes)) = self.untag(42) {
            Cid::try_from(&cid_bytes[1..]).map_err(|e| e.into())
        } else {
            Err(anyhow!("{} not a CID", self.type_name()))
        }
    }
}

impl TryInto<String> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<String, Self::Error> {
        if let CborValue::String(s) = self {
            Ok(s)
        } else {
            Err(anyhow!("{} not a String", self.type_name()))
        }
    }
}

impl Index<usize> for CborValue {
    type Output = Self;

    /// Returns a reference to the value corresponding to the supplied key.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the `Vec`.
    fn index(&self, index: usize) -> &Self {
        &self.as_array().unwrap()[index]
    }
}

impl Index<&CborValue> for CborValue {
    type Output = Self;

    /// Returns a reference to the value corresponding to the supplied key.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the `BTreeMap`.
    fn index(&self, key: &CborValue) -> &Self {
        &self.as_map().unwrap()[key]
    }
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
        let Ok(hash) = Multihash::from_bytes(&hash_bytes) else {
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

#[derive(Debug, Clone)]
pub struct SQLiteRootStore {
    pool: SqlitePool,
}

impl SQLiteRootStore {
    // ```sql
    // CREATE TABLE recon (root BLOB, timestamp INTEGER)
    // ```
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = Self { pool };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }

    async fn create_table_if_not_exists(&self) -> Result<()> {
        // this will need to be moved to migration logic if we ever change the schema
        sqlx::query("CREATE TABLE IF NOT EXISTS roots (root BLOB, timestamp INTEGER, PRIMARY KEY(root, timestamp));",
        )
        .execute(self.pool.writer())
        .await?;
        Ok(())
    }

    /// Store root, timestamp.
    pub async fn put(&self, root: &[u8], timestamp: i64) -> Result<()> {
        match sqlx::query("INSERT OR IGNORE INTO roots (root, timestamp) VALUES (?, ?)")
            .bind(root)
            .bind(timestamp)
            .execute(self.pool.writer())
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get(&self, root: &[u8]) -> Result<Option<i64>> {
        Ok(sqlx::query("SELECT timestamp FROM roots WHERE root = ?;")
            .bind(root)
            .fetch_optional(self.pool.reader())
            .await?
            .map(|row| row.get::<'_, i64, _>(0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash::{Code, MultihashDigest};

    #[tokio::test]
    async fn test_validate_time_event() {
        // Create an in-memory SQLite pool
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();

        // Create a new SQLiteBlockStore and SQLiteRootStore
        let block_store = SQLiteBlockStore::new(pool.clone()).await.unwrap();
        let root_store = SQLiteRootStore::new(pool.clone()).await.unwrap();

        // Add all the blocks for the Time Event to the block store
        let blocks = vec![
            "a4626964d82a58260001850112207ac18e1235f2f7a84548eeb543a33f89
             79eb88566a2dfc3d9596c55a683b7517647061746873302f302f302f302f
             302f302f302f302f302f306470726576d82a58260001850112207ac18e12
             35f2f7a84548eeb543a33f8979eb88566a2dfc3d9596c55a683b75176570
             726f6f66d82a58250001711220664fe7627b86f38a74cfbbcdb702d77fe9
             38533e5402b6ce867777078d706df5",
            "a464726f6f74d82a5825000171122041b6408c1b4be5016f652396ef47c0
             982c36d5877ebb874919bae3a9b854d8e166747848617368d82a58260001
             93011b20bf7bc715a09dea3177866ac4fc294ac9800ee2b49e09c55f5607
             8579bfbbf158667478547970656a6628627974657333322967636861696e
             4964686569703135353a31",
            "83d82a58250001711220e71cf20b225c878c15446675b1a209d14448eee5
             f51f5f378b192c7c64cfe1f0d82a5825000171122002920cb2496290eca3
             dff550072251d8b70b8995c243cf7808cbc305960c7962d82a5825000171
             12202d5794752351a770a7eba07e67a0d29119ac7a67fd32eacbffb690fc
             3e4f7ffb",
            "82d82a58250001711220b44e4681002e0c7bce248e9c91d49d85a3229aa7
             bdd1c43c7d245e3a7b8fc24ed82a582500017112206cf2b9460adabb65f5
             9369aeb45eaeab359501ea9a183efaf68c72af2dbaaa27",
            "82d82a582500017112200a07aa83a2ad17ed2809359d5c81cfd46213fa6b
             1a215639d3b973f122c8c04cd82a5825000171122079c9e24051148f5ec6
             6ceb6ea7c3ef24b3d219794a3ce738306380676eb92af0",
            "82d82a58250001711220a3261c31bfce9e22eb83ed305b017cb2b1b2edd2
             a90a294dd09f40201887020dd82a582500017112202096f43b3646196715
             a7cd7c503b020ebd21e1a4856952029782fb43abe3e54b",
            "82d82a58250001711220838a384925aa1d757b17b2a22607130d333efb75
             ab9523250d0d17c2e5cbbfc7d82a5825000171122035f019bfe0ae32bc3f
             47acc4babc8526f98185696fc3b5eb45757f8b05f7de0e",
            "82d82a58250001711220c0c93bdc49b93ac0786346a0118567ca66e4cfbd
             1d4c0519618c83ecbaa6e2aad82a582500017112202f3352cde99e1fe491
             18f2b5d598369add98957792c00b525e36757468086fcb",
            "82d82a582500017112204a813e0e1151f11776d02e88897fb615ae1ba3c6
             43fc5a486ed3378fa5fcf49dd82a58250001711220269d5384e44b53c54b
             5035b15bf13a8f4e3513e5ead4a23bfdeaa4af141ad37c",
            "82d82a582500017112201e48beab1c3fef5b29838492361155bd5c9c6389
             98bccc2da00625db5a9359cfd82a582500017112200d1bf984f229cddb85
             6fea0c8a6fd5c716defa3926b27b1ffc8308a29be4006c",
            "82d82a58250001711220015067f14cae18a15faebcacd1d07c1c3dcf2a24
             2334c7148de4d235f27308c6d82a58250001711220749e6401d5f860a457
             5c94f1742a8976f6d625fa47f1923132de758184e4b599",
            "82d82a58260001850112207ac18e1235f2f7a84548eeb543a33f8979eb88
             566a2dfc3d9596c55a683b7517d82a58260001850112209ef4cd6403d5ed
             4ebeb221809d141fbedb6686b6866a9c6e9230b802fd6353cd",
            "a3646c696e6bd82a58250001711220cb63d41a0489a815f44ee0a771bd70
             2f21a717bce67fcac4c4be0f14a25ad71f677061796c6f61647830415845
             53494d746a31426f45696167563945376770334739634338687078653835
             6e5f4b784d532d44785369577463666a7369676e61747572657381a26970
             726f74656374656478ac65794a68624763694f694a465a45525451534973
             496d74705a434936496d52705a4470725a586b36656a5a4e61326454566a
             4e30515856334e3264565633464c5131565a4e32466c4e6e5658546e6878
             5757646b6431426f56557069536d68474f5556475747303549336f325457
             746e5531597a64454631647a646e5656647853304e56575464685a545a31
             5630353463566c6e5a4864516146564b596b706f526a6c46526c68744f53
             4a39697369676e617475726578564f386c6f6358572d59657a56536b7976
             6773584976526b34773235536865337962517a4b5a466c386d706d684930
             3775543652356a5072624842664c36436a2d397a7061736b334643686b31
             38377269733374784177",
            "a26464617461a7646e616d6568426c657373696e67657669657773a16661
             7574686f72a164747970656f646f63756d656e744163636f756e74667363
             68656d61a66474797065666f626a656374652464656673a16a4772617068
             514c444944a4647479706566737472696e67657469746c656a4772617068
             514c444944677061747465726e788f5e6469643a5b612d7a412d5a302d39
             2e2123242526272a2b5c2f3d3f5e5f607b7c7d7e2d5d2b3a5b612d7a412d
             5a302d392e2123242526272a2b5c2f3d3f5e5f607b7c7d7e2d5d2a3a3f5b
             612d7a412d5a302d392e2123242526272a2b5c2f3d3f5e5f607b7c7d7e2d
             5d2a3a3f5b612d7a412d5a302d392e2123242526272a2b5c2f3d3f5e5f60
             7b7c7d7e2d5d2a24696d61784c656e67746818646724736368656d61782c
             68747470733a2f2f6a736f6e2d736368656d612e6f72672f64726166742f
             323032302d31322f736368656d616872657175697265648162746f6a7072
             6f70657274696573a262746fa1642472656672232f24646566732f477261
             7068514c4449446474657874a2647479706566737472696e67696d61784c
             656e67746818f0746164646974696f6e616c50726f70657274696573f467
             76657273696f6e63312e306972656c6174696f6e73a06b64657363726970
             74696f6e6a4120626c657373696e676f6163636f756e7452656c6174696f
             6ea16474797065646c69737466686561646572a363736570656d6f64656c
             656d6f64656c52ce01040171710b0009686d6f64656c2d76316b636f6e74
             726f6c6c6572738178386469643a6b65793a7a364d6b6753563374417577
             37675557714b4355593761653675574e7871596764775068554a624a6846
             394546586d39",
        ];
        for block in blocks {
            // Strip whitespace and decode the block from hex
            let block = hex::decode(block.replace(['\n', ' '], "")).unwrap();
            // Create the CID and store the block.
            let hash = Code::Sha2_256.digest(block.as_slice());
            let cid = Cid::new_v1(0x71, hash);
            block_store.put(cid, block.into(), vec![]).await.unwrap();
        }

        // Retrieve the Ethereum RPC URL from the environment, e.g. ETHEREUM_RPC_URL=https://mainnet.infura.io/v3/<api_key>
        let ethereum_rpc_url = std::env::var("ETHEREUM_RPC_URL").unwrap();
        assert_eq!(
            validate_time_event(
                &Cid::try_from("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy")
                    .unwrap(),
                &block_store,
                &root_store,
                ethereum_rpc_url.as_str(),
            )
            .await
            .unwrap(),
            1682958731
        );
        // Call validation a second time with an invalid Ethereum RPC URL. The validation should still work because the
        // result from the previous call was cached.
        assert_eq!(
            validate_time_event(
                &Cid::try_from("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy")
                    .unwrap(),
                &block_store,
                &root_store,
                "",
            )
            .await
            .unwrap(),
            1682958731
        );
    }
}
