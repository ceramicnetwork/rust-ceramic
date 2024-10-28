use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use arrow_flight::sql::client::FlightSqlServiceClient;
use ceramic_pipeline::cid_string::{CidString, CidStringList};
use clap::Args;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    error::DataFusionError,
    execution::context::SessionContext,
    functions_aggregate::first_last::LastValue,
    logical_expr::AggregateUDF,
};
use datafusion_cli::{exec::exec_from_repl, print_options::PrintOptions};
use datafusion_federation::sql::{SQLFederationProvider, SQLSchemaProvider};
use datafusion_flight_sql_table_provider::FlightSQLExecutor;
use tonic::transport::Endpoint;

#[derive(Args, Debug)]
pub struct QueryOpts {
    /// Query endpoint of the ceramic one daemon.
    #[arg(
        short,
        long,
        default_value = "http://127.0.0.1:5102",
        env = "CERAMIC_ONE_QUERY_ENDPOINT"
    )]
    query_endpoint: String,
}

pub async fn run(opts: QueryOpts) -> anyhow::Result<()> {
    // Setup federation
    let state = datafusion_federation::default_session_state();
    let client = new_client(opts.query_endpoint.clone()).await?;
    let executor = Arc::new(FlightSQLExecutor::new(opts.query_endpoint, client));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider =
        SQLSchemaProvider::new_with_tables(provider, vec!["conclusion_feed".to_string()])
            .await?
            .into();
    let mut ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ceramic", Arc::new(SQLCatalog { schema_provider }));

    ctx.register_udaf(AggregateUDF::new_from_impl(LastValue::default()));
    ctx.register_udf(CidString::new().into());
    ctx.register_udf(CidStringList::new().into());

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

/// Creates a new [FlightSqlServiceClient] for the passed endpoint. Completes the relevant auth configurations
/// or handshake as appropriate for the passed [FlightSQLAuth] variant.
async fn new_client(dsn: String) -> Result<FlightSqlServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::new(dsn).map_err(tx_error_to_df)?;
    let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn tx_error_to_df(err: tonic::transport::Error) -> DataFusionError {
    DataFusionError::External(format!("failed to connect: {err:?}").into())
}
struct SQLCatalog {
    schema_provider: Arc<SQLSchemaProvider>,
}

impl CatalogProvider for SQLCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["v0".to_string()]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.schema_provider.clone())
    }
}
