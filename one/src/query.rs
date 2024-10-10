use clap::Args;
use datafusion_cli::{exec::exec_from_repl, print_options::PrintOptions};
use object_store::aws::AmazonS3Builder;

#[derive(Args, Debug)]
pub struct QueryOpts {
    /// Endpoint of a Flight SQL server for the conclusion feed.
    #[arg(
        short,
        long,
        default_value = "http://127.0.0.1:5102",
        env = "CERAMIC_ONE_FLIGHT_SQL_ENDPOINT"
    )]
    flight_sql_endpoint: String,

    /// AWS S3 bucket name.
    /// When configured the aggregator will support storing data in S3 compatible object stores.
    ///
    /// Credentials are read from the environment:
    ///
    ///   * AWS_ACCESS_KEY_ID -> access_key_id
    ///   * AWS_SECRET_ACCESS_KEY -> secret_access_key
    ///   * AWS_DEFAULT_REGION -> region
    ///   * AWS_ENDPOINT -> endpoint
    ///   * AWS_SESSION_TOKEN -> token
    ///   * AWS_ALLOW_HTTP -> set to "true" to permit HTTP connections without TLS
    ///
    #[arg(long, env = "CERAMIC_ONE_AWS_BUCKET")]
    aws_bucket: String,
}

pub async fn run(opts: QueryOpts) -> anyhow::Result<()> {
    let mut ctx = ceramic_pipeline::session_from_config(opts).await?;

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

impl From<QueryOpts> for ceramic_pipeline::Config {
    fn from(value: QueryOpts) -> Self {
        Self {
            flight_sql_endpoint: value.flight_sql_endpoint,
            aws_s3_builder: AmazonS3Builder::from_env().with_bucket_name(&value.aws_bucket),
        }
    }
}
