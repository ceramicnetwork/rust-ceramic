use crate::ethereum_rpc::eth_transaction_by_hash;
use crate::CborValue;
use anyhow::{anyhow, Result};
use ceramic_core::{ssi, Base64UrlString, DidDocument, Jwk};
use ceramic_p2p::SQLiteBlockStore;
use ceramic_store::SqlitePool;
use chrono::{SecondsFormat, TimeZone, Utc};
use cid::{multibase, multihash, Cid};
use clap::{Args, Subcommand};
use glob::{glob, Paths};
use multihash::Multihash;
use sqlx::Row;
use std::{fs, path::PathBuf, str::FromStr};
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
    let pool = SqlitePool::from_store_dir(opts.store_dir).await.unwrap();
    let block_store = SQLiteBlockStore::new(pool.clone()).await.unwrap();
    let root_store = SQLiteRootStore::new(pool).await.unwrap();

    // Validate that the CID is either DAG-CBOR or DAG-JOSE
    if (opts.cid.codec() != 0x71) && (opts.cid.codec() != 0x85) {
        return Err(anyhow!("CID {} is not a valid Ceramic event", opts.cid));
    }

    // If the CID is a DAG-JOSE, the event is either a signed Data Event or a signed Init Event, both of which can be
    // validated similarly.
    if opts.cid.codec() == 0x85 {
        let controller = validate_data_event_envelope(&opts.cid, &block_store).await;
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
            validate_data_event_payload(&opts.cid, &block_store, None).await?;
        }
    }
    Ok(())
}

async fn validate_data_event_envelope(cid: &Cid, block_store: &SQLiteBlockStore) -> Result<String> {
    let block = block_store.get(cid.to_owned()).await?.unwrap();
    let envelope = CborValue::parse(&block)?;
    let signatures_0 = envelope
        .get_key("signatures")
        .unwrap()
        .get_index(0)
        .unwrap();
    let protected = signatures_0
        .get_key("protected")
        .unwrap()
        .as_bytes()
        .unwrap()
        .as_slice();
    // Deserialize protected as JSON
    let protected_json: serde_json::Value = serde_json::from_slice(&protected)?;
    let controller = protected_json.get("kid").unwrap().as_str().unwrap();
    let protected: Base64UrlString = protected.try_into()?;
    let signature: Base64UrlString = signatures_0
        .get_key("signature")
        .unwrap()
        .as_bytes()
        .unwrap()
        .as_slice()
        .try_into()?;
    let payload: Base64UrlString = envelope
        .get_key("payload")
        .unwrap()
        .as_bytes()
        .unwrap()
        .as_slice()
        .try_into()?;
    let compact = format!("{}.{}.{}", protected, payload, signature);
    let did = DidDocument::new(&controller);
    let jwk = Jwk::new(&did).await.unwrap();
    ssi::jws::decode_verify(&compact, &jwk)?;
    validate_data_event_payload(cid, block_store, Some(did.id.clone())).await?;
    Ok(did.id)
}

// TODO: Validate the Data Event payload structure
// TODO: Validate CACAO
async fn validate_data_event_payload(
    _payload_cid: &Cid,
    _block_store: &SQLiteBlockStore,
    _controller: Option<String>,
) -> Result<()> {
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

        // Add all the blocks for the Data Event & Time Event to the block store
        let blocks = vec![
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy
            "a4626964d82a58260001850112207ac18e1235f2f7a84548eeb543a33f89
             79eb88566a2dfc3d9596c55a683b7517647061746873302f302f302f302f
             302f302f302f302f302f306470726576d82a58260001850112207ac18e12
             35f2f7a84548eeb543a33f8979eb88566a2dfc3d9596c55a683b75176570
             726f6f66d82a58250001711220664fe7627b86f38a74cfbbcdb702d77fe9
             38533e5402b6ce867777078d706df5",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/
            "a464726f6f74d82a5825000171122041b6408c1b4be5016f652396ef47c0
             982c36d5877ebb874919bae3a9b854d8e166747848617368d82a58260001
             93011b20bf7bc715a09dea3177866ac4fc294ac9800ee2b49e09c55f5607
             8579bfbbf158667478547970656a6628627974657333322967636861696e
             4964686569703135353a31",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/
            "83d82a58250001711220e71cf20b225c878c15446675b1a209d14448eee5
             f51f5f378b192c7c64cfe1f0d82a5825000171122002920cb2496290eca3
             dff550072251d8b70b8995c243cf7808cbc305960c7962d82a5825000171
             12202d5794752351a770a7eba07e67a0d29119ac7a67fd32eacbffb690fc
             3e4f7ffb",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/
            "82d82a58250001711220b44e4681002e0c7bce248e9c91d49d85a3229aa7
             bdd1c43c7d245e3a7b8fc24ed82a582500017112206cf2b9460adabb65f5
             9369aeb45eaeab359501ea9a183efaf68c72af2dbaaa27",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/
            "82d82a582500017112200a07aa83a2ad17ed2809359d5c81cfd46213fa6b
             1a215639d3b973f122c8c04cd82a5825000171122079c9e24051148f5ec6
             6ceb6ea7c3ef24b3d219794a3ce738306380676eb92af0",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/
            "82d82a58250001711220a3261c31bfce9e22eb83ed305b017cb2b1b2edd2
             a90a294dd09f40201887020dd82a582500017112202096f43b3646196715
             a7cd7c503b020ebd21e1a4856952029782fb43abe3e54b",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/
            "82d82a58250001711220838a384925aa1d757b17b2a22607130d333efb75
             ab9523250d0d17c2e5cbbfc7d82a5825000171122035f019bfe0ae32bc3f
             47acc4babc8526f98185696fc3b5eb45757f8b05f7de0e",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/
            "82d82a58250001711220c0c93bdc49b93ac0786346a0118567ca66e4cfbd
             1d4c0519618c83ecbaa6e2aad82a582500017112202f3352cde99e1fe491
             18f2b5d598369add98957792c00b525e36757468086fcb",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/
            "82d82a582500017112204a813e0e1151f11776d02e88897fb615ae1ba3c6
             43fc5a486ed3378fa5fcf49dd82a58250001711220269d5384e44b53c54b
             5035b15bf13a8f4e3513e5ead4a23bfdeaa4af141ad37c",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/0/
            "82d82a582500017112201e48beab1c3fef5b29838492361155bd5c9c6389
             98bccc2da00625db5a9359cfd82a582500017112200d1bf984f229cddb85
             6fea0c8a6fd5c716defa3926b27b1ffc8308a29be4006c",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/0/0/
            "82d82a58250001711220015067f14cae18a15faebcacd1d07c1c3dcf2a24
             2334c7148de4d235f27308c6d82a58250001711220749e6401d5f860a457
             5c94f1742a8976f6d625fa47f1923132de758184e4b599",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/0/0/0/
            "82d82a58260001850112207ac18e1235f2f7a84548eeb543a33f8979eb88
             566a2dfc3d9596c55a683b7517d82a58260001850112209ef4cd6403d5ed
             4ebeb221809d141fbedb6686b6866a9c6e9230b802fd6353cd",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/0/0/0/0/
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/prev
            "a2677061796c6f6164582401711220cb63d41a0489a815f44ee0a771bd70
             2f21a717bce67fcac4c4be0f14a25ad71f6a7369676e61747572657381a2
             6970726f74656374656458817b22616c67223a224564445341222c226b69
             64223a226469643a6b65793a7a364d6b675356337441757737675557714b
             4355593761653675574e7871596764775068554a624a6846394546586d39
             237a364d6b675356337441757737675557714b4355593761653675574e78
             71596764775068554a624a6846394546586d39227d697369676e61747572
             6558403bc9687175be61ecd54a4caf82c5c8bd1938c36e5285edf26d0cca
             64597c9a99a1234eee4fa4798cfadb1c17cbe828fef73a5ab24dc50a1935
             f3bae2b37b7103",
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/proof/root/0/0/0/0/0/0/0/0/0/0/link
            // bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy/prev/link
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
                &Cid::try_from("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy") // cspell:disable-line
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
                &Cid::try_from("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy") // cspell:disable-line
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
