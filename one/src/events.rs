use anyhow::{anyhow, Error, Result};
use ceramic_p2p::SQLiteBlockStore;
use ceramic_store::SqlitePool;
use chrono::{SecondsFormat, Utc};
use cid::{multibase, multihash, Cid};
use clap::{Args, Subcommand};
use glob::{glob, Paths};
use minicbor::{data::Tag, data::Type, display, Decoder};
use multihash::Multihash;
use ordered_float::OrderedFloat;
use sqlx::Row;
use std::collections::BTreeMap;
use std::{fs, path::PathBuf};

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

    /// Ethereum RPC URL
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

    let timestamp = validate_time_event(&opts.cid, &block_store, &root_store, opts.ethereum_rpc_url.as_str()).await?;
    println!("proof validated at timestamp: {}", timestamp);
    Ok(())
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
        // Destructure the proof to get the tag and the value
        let proof_cid: Cid = CborValue::parse(&block)?.path(&["proof"]).try_into()?;
        if let Some(proof_block) = block_store.get(proof_cid).await? {
            println!("proof block: {}", display(&proof_block));
            let proof_cbor = CborValue::parse(&proof_block)?;
            let proof_root: Cid = proof_cbor.path(&["root"]).try_into()?;
            // if root in root_store return timestamp.
            // note: at some point we will need a negative cache to exponentially backoff eth_getTransactionByHash
            if let Ok(Some(timestamp)) = root_store.get(proof_root.hash().digest()).await {
                return Ok(timestamp);
            }
            // else eth_transaction_by_hash


            let tx_hash_cid: Cid = proof_cbor.path(&["txHash"]).try_into()?;
            

            let (transaction_root, timestamp) =
                eth_transaction_by_hash(tx_hash_cid, ethereum_rpc_url).await?;
            println!("root: {}, timestamp: {}", transaction_root, timestamp);

            if transaction_root == proof_root{
                // TODO validate that prev is in root
                root_store.put(transaction_root.hash().digest(), timestamp).await?;
                Ok(timestamp)
            } else {
                Err(anyhow!("root from transaction {} != root from proof {}", transaction_root, proof_root))
            }
        } else {
            Err(anyhow!("proof {} not found", cid))
        }
    } else {
        println!("CID: {} not found", cid);
        Err(anyhow!("CID {} not found", cid))
    } 
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
    println!("json: {}", json);
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
    println!("json: {}", json);
    if let Some(result) = json.get("result") {
        if let Some(timestamp) = result.get("timestamp") {
            if let Some(timestamp) = timestamp.as_str() {
                return get_timestamp_from_hex_string(timestamp);
            }
        }
    }
    Err(anyhow!("missing fields"))
}

fn get_root_from_input(input: &str) -> Result<Cid> {
    if input.starts_with("0x97ad09eb") {
        // Strip "0x97ad09eb" from the input and convert it into a cidv1 - dag-cbor - (sha2-256 : 256)
        // 0x12 -> sha2-256
        // 0x20 -> 256 bits of hash
        let mut root_bytes = vec![0x12_u8, 0x20];
        root_bytes.append(hex::decode(&input[10..])?.as_mut());
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

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug)]
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
            println!("decoder not at end {}/{}", decoder.position(), bytes.len());
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

#[derive(Debug, Clone)]
pub struct SQLiteRootStore {
    pool: SqlitePool,
}

impl SQLiteRootStore {
    // ```sql
    // CREATE TABLE IF NOT EXISTS recon (root BLOB, timestamp INTEGER PRIMARY KEY(root, timestamp))
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
            .map(|row| row.get::<'_, i64, _>(0).into()))
    }
}