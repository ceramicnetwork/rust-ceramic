use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use async_trait::async_trait;
use ceramic_event::unvalidated;
use ceramic_event_svc::{BlockStore, EventService};
use ceramic_metrics::config::Config as MetricsConfig;
use cid::Cid;
use clap::{Args, Subcommand};
use futures::{stream::BoxStream, StreamExt};
use multihash_codetable::{Code, Multihash, MultihashDigest};
use tracing::{debug, info, trace};

use crate::{default_directory, DBOpts, Info, LogOpts};

#[derive(Subcommand, Debug)]
pub enum EventsCommand {
    /// Migrate raw event blocks from IPFS.
    FromIpfs(FromIpfsOpts),
}

impl EventsCommand {
    fn log_format(&self) -> ceramic_metrics::config::LogFormat {
        match self {
            EventsCommand::FromIpfs(opts) => opts.log_opts.format(),
        }
    }
}

#[derive(Args, Debug)]
pub struct FromIpfsOpts {
    /// The path to the ipfs_repo [eg: ~/.ipfs/blocks]
    #[clap(long, short, value_parser, env = "CERAMIC_ONE_INPUT_IPFS_PATH")]
    input_ipfs_path: PathBuf,

    /// Path to storage directory
    #[clap(
        long,
        short,
        default_value=default_directory().into_os_string(),
        env = "CERAMIC_ONE_OUTPUT_STORE_PATH"
    )]
    output_store_path: PathBuf,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: crate::Network,

    /// Unique id when the network type is 'local'.
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,

    /// Expect non sharded paths to blocks.
    /// Sharded paths are organized into a two character prefix directories.
    #[arg(long, env = "CERAMIC_ONE_NON_SHARDED_PATHS", default_value_t = false)]
    non_sharded_paths: bool,

    /// Log information about tile documents found during the migration.
    #[arg(long, env = "CERAMIC_ONE_LOG_TILE_DOCS", default_value_t = false)]
    log_tile_docs: bool,

    #[command(flatten)]
    log_opts: LogOpts,
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
        log_format: cmd.log_format(),
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
    let sqlite_pool = db_opts.get_sqlite_pool().await?;
    // TODO: feature flags here? or just remove this entirely when enabling
    let event_svc = Arc::new(EventService::try_new(sqlite_pool, false, false, None).await?);
    let blocks = FSBlockStore {
        input_ipfs_path: opts.input_ipfs_path,
        sharded_paths: !opts.non_sharded_paths,
    };
    event_svc
        .migrate_from_ipfs(network, blocks, opts.log_tile_docs)
        .await?;
    Ok(())
}

struct FSBlockStore {
    input_ipfs_path: PathBuf,
    sharded_paths: bool,
}

impl FSBlockStore {
    fn sharded_block_path(&self, cid: &Cid) -> Result<PathBuf> {
        let path = self.input_ipfs_path.clone();
        // 1. Create v0 CID throwing away the codec
        let v0 = Cid::new_v0(*cid.hash())?;
        // 2. Determine the base32 encoding of the v0 CID bytes
        let base32_string = multibase::encode(multibase::Base::Base32Upper, v0.to_bytes());
        // 3. Get the two characters prefix for this CID
        let len = base32_string.len();
        let prefix = &base32_string[len - 3..len - 1];
        // 4. Construct a path as `{ROOT}/{PREFIX}/{base32 without B}.data`
        Ok(path
            .join(prefix)
            .join(base32_string.trim_start_matches('B'))
            .with_extension("data"))
    }
    fn non_sharded_block_path(&self, cid: &Cid) -> Result<PathBuf> {
        let path = self.input_ipfs_path.clone();
        // 1. Create v0 CID throwing away the codec
        let v0 = Cid::new_v0(*cid.hash())?;
        // 2. Determine the base32 encoding of the v0 CID bytes
        let base32_string = multibase::encode(multibase::Base::Base32Upper, v0.to_bytes());
        // 3. Construct a path as `{ROOT}/{base32 without B}`
        Ok(path.join(base32_string.trim_start_matches('B')))
    }
}

#[async_trait]
impl BlockStore for FSBlockStore {
    fn blocks(&self) -> BoxStream<'static, anyhow::Result<(Cid, Vec<u8>)>> {
        // the block store is split in to 1024 directories and then the blocks stored as files.
        // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
        // The leading "B" for the b32 sha256 multihash is left off
        // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
        info!(path = %self.input_ipfs_path.display(), "opening IPFS repo");

        let mut dirs = Vec::new();
        dirs.push(self.input_ipfs_path.clone());

        try_stream! {
            while let Some(dir) = dirs.pop() {
                let mut entries = tokio::fs::read_dir(dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    if entry.metadata().await?.is_dir() {
                        dirs.push(entry.path())
                    } else if let Ok(Some(block)) = block_from_path(entry.path()).await{
                        yield block
                    } else {
                        trace!(path = entry.path().display().to_string(), "skipping non-block file");
                        continue;
                    }
                }
            }
        }
        .boxed()
    }
    async fn block_data(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let path = if self.sharded_paths {
            self.sharded_block_path(cid)?
        } else {
            self.non_sharded_block_path(cid)?
        };

        if tokio::fs::try_exists(&path).await? {
            Ok(Some(tokio::fs::read(&path).await?))
        } else {
            Ok(None)
        }
    }
}

async fn block_from_path(block_path: PathBuf) -> Result<Option<(Cid, Vec<u8>)>> {
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
    Ok(Some((cid, blob)))
}
