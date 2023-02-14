#![deny(warnings)]
use std::path::PathBuf;

use anyhow::{bail, Result};
use futures_util::StreamExt;
use iroh_api::{IpfsPath, OutType};
use iroh_embed::{
    IrohBuilder, Libp2pConfig, P2pServiceBuilder, P2pServiceBuilderVanilla, RocksStoreService,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let dir = PathBuf::from("/tmp/rust-ceramic");
    println!("Using directory: {}", dir.display());

    println!("Starting iroh system...");
    let store = RocksStoreService::new(dir.join("store")).await?;

    let mut p2p_config = Libp2pConfig::default();
    p2p_config.listening_multiaddrs = vec![
        "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
        "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
    ];
    let behaviour = ceramic_set_rec::Behaviour::default();
    let p2p_builder = P2pServiceBuilderVanilla::new(p2p_config, dir, store.addr())
        .with_custom_behaviour(Some(behaviour));
    let p2p = p2p_builder.build().await?;

    // Note by default this is configured with an indexer, but not with http resolvers.
    let iroh = IrohBuilder::new().store(store).p2p(p2p).build().await?;
    println!("done");

    let quick_start: IpfsPath =
        "/ipfs/QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/quick-start".parse()?;
    println!("Fetching quick start: {quick_start}");
    let mut stream = iroh.api().get(&quick_start)?;

    // We only expect a single item here.
    while let Some(item) = stream.next().await {
        let (rel_path, data) = item?;
        println!("PATH: {rel_path}");
        println!("----");
        match data {
            OutType::Dir => bail!("found unexpected dir"),
            OutType::Symlink(_) => bail!("found unexpected symlink"),
            OutType::Reader(mut reader) => {
                let mut stdout = tokio::io::stdout();
                tokio::io::copy(&mut reader, &mut stdout).await?;
            }
        }
    }

    // Stop the system gracefully.
    iroh.stop().await?;

    Ok(())
}
