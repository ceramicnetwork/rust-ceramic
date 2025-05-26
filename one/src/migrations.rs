use std::{path::PathBuf, str::FromStr as _, sync::Arc};

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use async_trait::async_trait;
use ceramic_core::StreamId;
use ceramic_event::unvalidated;
use ceramic_event_svc::{BlockStore, EventService};
use ceramic_metrics::config::Config as MetricsConfig;
use ceramic_sql::sqlite::SqliteOpts;
use cid::Cid;
use clap::{Args, Subcommand};
use futures::{stream::BoxStream, StreamExt};
use multihash_codetable::{Code, Multihash, MultihashDigest};
use tokio::io::AsyncBufReadExt;
use tracing::{debug, info};

use crate::{default_directory, DBOpts, Info, LogOpts, Network};

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
        default_value = default_directory().into_os_string(),
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

    /// Path of file containing list of newline-delimited file paths to migrate.
    ///
    /// See below for example usage when running a migration for a live IPFS node. Multiple migration runs using lists
    /// of files that have changed between runs is useful for incremental migrations. This method can also be used for
    /// a final migration after shutting down the IPFS node so that all inflight blocks are migrated to the new C1 node.
    ///
    /// # Get list of files in sorted order
    ///
    /// find ~/.ipfs/blocks -type f | sort > first_run_files.txt
    ///
    /// # Run the migration
    ///
    /// ceramic-one migrations from-ipfs --input-ipfs-path ~/.ipfs/blocks --input-file-list-path first_run_files.txt
    ///
    /// # Get updated list of files in sorted order
    ///
    /// find ~/.ipfs/blocks -type f | sort > second_run_files.txt
    ///
    /// # Use comm to get the list of new files
    ///
    /// comm -13 first_run_files.txt second_run_files.txt > new_files.txt
    ///
    /// # Re-run the migration
    ///
    /// ceramic-one migrations from-ipfs --input-ipfs-path ~/.ipfs/blocks --input-file-list-path new_files.txt
    #[clap(long, short = 'f', env = "CERAMIC_ONE_INPUT_FILE_LIST_PATH")]
    input_file_list_path: Option<PathBuf>,

    /// Offset within the input files to start from
    #[clap(
        long,
        short = 's',
        default_value = "0",
        env = "CERAMIC_ONE_MIGRATION_OFFSET"
    )]
    offset: u64,

    /// Number of files to process
    #[clap(long, short = 'l', env = "CERAMIC_ONE_MIGRATION_LIMIT")]
    limit: Option<u64>,

    /// Optional list of model stream ids. Only events from these models will be migrated.
    /// If the list is empty all events are migrated.
    /// The 'metamodel' stream id is `kh4q0ozorrgaq2mezktnrmdwleo1d` if you want to migrate models.
    #[clap(long, value_delimiter = ',', env = "CERAMIC_ONE_MODEL_FILTER")]
    model_filter: Vec<String>,

    /// Whether to validate the signatures of signed events.
    /// Events with invalid signatures will be skipped and counted as errors.
    #[clap(long, env = "CERAMIC_ONE_VALIDATE_SIGNATURES")]
    validate_signatures: bool,

    /// Whether to validate the chain of time events matches the expected CAIP2 chain id.
    /// Events with invalid chains will be skipped and counted as errors.
    /// For example, on mainnet, events are expected to have a chain id of 'eip155:1'.
    #[clap(long, env = "CERAMIC_ONE_VALIDATE_CHAIN")]
    validate_chain: bool,

    /// Ethereum RPC URLs used for time events validation. Required when connecting to mainnet and uses fallback URLs if not specified for other networks.
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_ETHEREUM_RPC_URLS"
    )]
    ethereum_rpc_urls: Vec<String>,
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
    // Limit and offset are only used when reading from a file list
    if (opts.limit.is_some() || opts.offset > 0) && opts.input_file_list_path.is_none() {
        return Err(anyhow!(
            "File list path is required when using limit or offset"
        ));
    }
    let network = opts.network.to_network(&opts.local_network_id)?;
    let db_opts: DBOpts = (&opts).into();
    let sqlite_pool = db_opts.get_sqlite_pool(SqliteOpts::default()).await?;
    let rpc_providers = opts
        .network
        .get_eth_rpc_providers(opts.ethereum_rpc_urls)
        .await?;

    // TODO: feature flags here? or just remove this entirely when enabling
    let event_svc = Arc::new(
        EventService::try_new(
            sqlite_pool,
            ceramic_event_svc::UndeliveredEventReview::Skip,
            false,
            rpc_providers,
        )
        .await?,
    );
    let blocks = FSBlockStore {
        input_ipfs_path: opts.input_ipfs_path,
        sharded_paths: !opts.non_sharded_paths,
        input_file_list_path: opts.input_file_list_path,
        file_offset: opts.offset,
        file_limit: opts.limit,
    };
    event_svc
        .migrate_from_ipfs(
            network,
            blocks,
            opts.log_tile_docs,
            opts.model_filter
                .iter()
                .map(|model| {
                    StreamId::from_str(model)
                        .map_err(|err| anyhow!("model filter must be valid stream id: {err}"))
                        .map(|s| s.to_vec())
                })
                .collect::<Result<Vec<Vec<u8>>, _>>()?,
            opts.validate_signatures,
            opts.validate_chain
                .then(|| allowed_chains(opts.network))
                .flatten(),
        )
        .await?;
    Ok(())
}

/// Returns the allowed chain ids (historically) for the given network.
/// [`Network::supported_chain_ids`] is for current chains
fn allowed_chains(network: Network) -> Option<Vec<String>> {
    match network {
        Network::Mainnet => Some(vec!["eip155:1".to_string()]),
        Network::TestnetClay => Some(vec![
            "eip155:3".to_string(),
            "eip155:5".to_string(),
            "eip155:100".to_string(),
        ]),
        Network::DevUnstable => Some(vec!["eip155:11155111".to_string()]),
        Network::Local => None,
        Network::InMemory => None,
    }
}

struct FSBlockStore {
    input_ipfs_path: PathBuf,
    sharded_paths: bool,
    input_file_list_path: Option<PathBuf>,
    file_offset: u64,
    file_limit: Option<u64>,
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
        match &self.input_file_list_path {
            Some(_) => self.blocks_from_list(),
            None => self.blocks_from_dir(),
        }
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

impl FSBlockStore {
    fn blocks_from_dir(&self) -> BoxStream<'static, Result<(Cid, Vec<u8>)>> {
        // the block store is split in to 1024 directories and then the blocks stored as files.
        // the dir structure is the penultimate two characters as dir then the b32 sha256 multihash of the block
        // The leading "B" for the b32 sha256 multihash is left off
        // ~/.ipfs/blocks/QV/CIQOHMGEIKMPYHAUTL57JSEZN64SIJ5OIHSGJG4TJSSJLGI3PBJLQVI.data // cspell:disable-line
        info!(path = %self.input_ipfs_path.display(), "opening IPFS repo");

        let mut dirs = Vec::new();
        dirs.push(self.input_ipfs_path.clone());

        (try_stream! {
            while let Some(dir) = dirs.pop() {
                let mut entries = tokio::fs::read_dir(dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    if entry.metadata().await?.is_dir() {
                        dirs.push(entry.path())
                    } else if let Some(block) = block_from_path(entry.path()).await?{
                        yield block
                    }
                }
            }
        })
        .boxed()
    }

    fn blocks_from_list(&self) -> BoxStream<'static, Result<(Cid, Vec<u8>)>> {
        let input_file_list_path = self
            .input_file_list_path
            .clone()
            .expect("input_file_list_path should have been checked before calling this function");
        info!(path = %input_file_list_path.display(), "reading IPFS file list");

        let offset = self.file_offset;
        let limit = self.file_limit;
        (try_stream! {
            let file = tokio::fs::File::open(input_file_list_path).await?;
            let lines = tokio::io::BufReader::new(file).lines();
            let lines = tokio_stream::wrappers::LinesStream::new(lines);
            let mut lines = lines.skip(offset as usize).take(limit.unwrap_or(u64::MAX) as usize);
            while let Some(line) = lines.next().await.transpose()? {
                let path = PathBuf::from(line);
                match block_from_path(path.clone()).await {
                    Ok(Some(block)) => yield block,
                    Ok(None) => {},
                    Err(e) => {
                        debug!(path = %path.display(), "error reading block: {:#}", e);
                    },
                }
            }
        })
        .boxed()
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
        code => {
            return Err(anyhow!("unsupported hash {code}"));
        }
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
