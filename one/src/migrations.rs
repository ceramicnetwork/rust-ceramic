use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use async_trait::async_trait;
use ceramic_event::unvalidated;
use ceramic_metrics::config::Config as MetricsConfig;
use ceramic_service::{Block, BoxedBlock};
use cid::Cid;
use clap::{Args, Subcommand};
use futures::Stream;
use multihash_codetable::{Code, Multihash, MultihashDigest};
use tracing::{debug, info};

use crate::{DBOpts, Info};

#[derive(Subcommand, Debug)]
pub enum EventsCommand {
    /// Migrate raw event blocks from IPFS.
    FromIpfs(FromIpfsOpts),
}

#[derive(Args, Debug)]
pub struct FromIpfsOpts {
    /// The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    #[clap(long, short, value_parser, env = "CERAMIC_ONE_INPUT_IPFS_PATH")]
    input_ipfs_path: PathBuf,

    /// Path to storage directory
    #[clap(long, short, env = "CERAMIC_ONE_OUTPUT_STORE_PATH")]
    output_store_path: Option<PathBuf>,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: crate::Network,

    /// Unique id when the network type is 'local'.
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,
}

impl From<&FromIpfsOpts> for DBOpts {
    fn from(value: &FromIpfsOpts) -> Self {
        Self {
            store_dir: value.output_store_path.clone(),
        }
    }
}

pub async fn migrate(cmd: EventsCommand) -> Result<()> {
    let info = Info::new().await?;
    let mut metrics_config = MetricsConfig {
        export: false,
        tracing: false,
        log_format: ceramic_metrics::config::LogFormat::MultiLine,
        ..Default::default()
    };
    info.apply_to_metrics_config(&mut metrics_config);
    // Logging Tracing and metrics are initialized here,
    // debug,info etc will not work until after this line
    let metrics_handle = ceramic_metrics::MetricsHandle::new(metrics_config.clone())
        .await
        .expect("failed to initialize metrics");
    match cmd {
        EventsCommand::FromIpfs(opts) => from_ipfs(opts).await?,
    }
    metrics_handle.shutdown();
    debug!("metrics server stopped");
    Ok(())
}

async fn from_ipfs(opts: FromIpfsOpts) -> Result<()> {
    let network = opts.network.to_network(&opts.local_network_id)?;
    let db_opts: DBOpts = (&opts).into();
    let crate::Databases::Sqlite(db) = db_opts.get_database().await?;
    let blocks = blocks_from_filesystem(opts.input_ipfs_path);
    db.event_store.migrate_from_ipfs(network, blocks).await?;
    Ok(())
}
fn blocks_from_filesystem(input_ipfs_path: PathBuf) -> impl Stream<Item = Result<BoxedBlock>> {
    // the block store is split in to 1024 directories and then the blocks stored as files.
    // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
    // The leading "B" for the b32 sha256 multihash is left off
    // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
    info!(path = %input_ipfs_path.display(), "opening IPFS repo");

    let mut dirs = Vec::new();
    dirs.push(input_ipfs_path);

    try_stream! {
        while !dirs.is_empty() {
            let mut entries = tokio::fs::read_dir(dirs.pop().unwrap()).await?;
            while let Some(entry) = entries.next_entry().await? {
                if entry.metadata().await?.is_dir() {
                    dirs.push(entry.path())
                } else if let Some(block) = block_from_path(entry.path()).await?{
                    yield block
                }
            }
        }
    }
}
async fn block_from_path(block_path: PathBuf) -> Result<Option<BoxedBlock>> {
    if !block_path.is_file() {
        return Ok(None);
    }

    let Ok((_base, hash_bytes)) =
        multibase::decode("B".to_string() + block_path.file_stem().unwrap().to_str().unwrap())
    else {
        debug!(path = %block_path.display(), "block filename is not valid base32upper");
        return Ok(None);
    };
    let Ok(hash) = Multihash::from_bytes(&hash_bytes) else {
        debug!(path = %block_path.display(), "block filename is not a valid multihash");
        return Ok(None);
    };
    let blob = tokio::fs::read(&block_path).await?;
    let blob_hash = match hash.code() {
        0x12 => Code::Sha2_256.digest(&blob),
        code => return Err(anyhow!("unsupported hash {code}")),
    };
    if blob_hash != hash {
        return Err(anyhow!(
            "block data does not match hash: path={}",
            block_path.display()
        ));
    }
    // If we can decode the block as a JWS envelope then we can assume the block is dag-jose
    // encoded.
    const DAG_CBOR: u64 = 0x71;
    const DAG_JOSE: u64 = 0x85;
    let result: Result<unvalidated::signed::Envelope, _> = serde_ipld_dagcbor::from_slice(&blob);
    let cid = if result.is_ok() {
        Cid::new_v1(DAG_JOSE, hash)
    } else {
        Cid::new_v1(DAG_CBOR, hash)
    };
    Ok(Some(Box::new(FSBlock {
        cid,
        path: block_path,
    })))
}
struct FSBlock {
    cid: Cid,
    path: PathBuf,
}
#[async_trait]
impl Block for FSBlock {
    fn cid(&self) -> Cid {
        self.cid
    }
    async fn data(&self) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(&self.path).await?)
    }
}
