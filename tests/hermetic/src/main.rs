use anyhow::Result;
use clap::Parser;
use hermetic_driver::cli;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = cli::Cli::parse();
    match args.command {
        cli::Command::Test(opts) => cli::tester::run(opts.try_into()?).await,
    }
}
