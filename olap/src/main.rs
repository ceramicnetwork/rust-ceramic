#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    ceramic_olap::run().await.map_err(|e| {
        // this should use stderr if we error before tracing is hooked up
        tracing::error!("Error running command: {:#}", e);
        e
    })
}
