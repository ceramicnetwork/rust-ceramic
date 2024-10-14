//! Pipeline provides a set of tables of Ceramic events and transformations between them.

pub mod cid_string;
#[warn(missing_docs)]
mod config;
pub mod schemas;

use std::{any::Any, sync::Arc};

use anyhow::{anyhow, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use cid_string::{CidString, CidStringList};
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    error::DataFusionError,
    execution::context::SessionContext,
    functions_aggregate::first_last::LastValue,
    logical_expr::{col, AggregateUDF, ScalarUDF},
};
use datafusion_federation::sql::{SQLFederationProvider, SQLSchemaProvider};
use datafusion_flight_sql_table_provider::FlightSQLExecutor;
use object_store::aws::AmazonS3ConfigKey;
use tonic::transport::Endpoint;
use url::Url;

pub use config::Config;

/// Constructs a [`SessionContext`] configured with all tables in the pipeline.
pub async fn session_from_config(config: impl Into<Config>) -> Result<SessionContext> {
    let config: Config = config.into();

    // Create federated datafusion state
    let state = datafusion_federation::default_session_state();
    let client = new_client(config.flight_sql_endpoint.clone()).await?;
    let executor = Arc::new(FlightSQLExecutor::new(config.flight_sql_endpoint, client));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider = Arc::new(
        SQLSchemaProvider::new_with_tables(provider, vec!["conclusion_feed".to_string()]).await?,
    );

    // Create datafusion context
    let ctx = SessionContext::new_with_state(state);

    // Register various UDxFs
    ctx.register_udaf(AggregateUDF::new_from_impl(LastValue::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidStringList::new()));

    // Register s3 object store
    let bucket = config
        .aws_s3_builder
        .get_config_value(&AmazonS3ConfigKey::Bucket)
        .ok_or_else(|| anyhow!("AWS S3 bucket must be specified"))?;
    let s3 = config.aws_s3_builder.build()?;
    let mut url = Url::parse("s3://")?;
    url.set_host(Some(&bucket))?;
    ctx.register_object_store(&url, Arc::new(s3));

    // Register federated catalog
    ctx.register_catalog("ceramic", Arc::new(SQLCatalog { schema_provider }));

    // Configure doc_state listing table
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_file_sort_order(vec![vec![col("index").sort(true, true)]]);

    // Set the path within the bucket for the doc_state table
    const DOC_STATE_OBJECT_STORE_PATH: &str = "/ceramic/v0/doc_state/";
    url.set_path(DOC_STATE_OBJECT_STORE_PATH);
    ctx.register_listing_table(
        "doc_state",
        url.to_string(),
        listing_options,
        Some(schemas::doc_state()),
        None,
    )
    .await?;
    Ok(ctx)
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
