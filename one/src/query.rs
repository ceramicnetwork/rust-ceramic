use datafusion::prelude::SessionContext;
use datafusion_cli::{exec::exec_from_repl, print_options::PrintOptions};

pub async fn run() -> anyhow::Result<()> {
    let mut ctx = SessionContext::default();
    datafusion_functions_json::register_all(&mut ctx)?;

    let mut print_options = PrintOptions {
        format: datafusion_cli::print_format::PrintFormat::Automatic,
        quiet: false,
        maxrows: datafusion_cli::print_options::MaxRows::Unlimited,
        color: true,
    };

    exec_from_repl(&ctx, &mut print_options).await.unwrap();

    Ok(())
}
