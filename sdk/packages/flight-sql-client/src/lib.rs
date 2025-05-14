#![deny(clippy::all)]

mod conversion;
mod error;
mod feed_query;
mod flight_client;

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Datum as _, RecordBatch, StringArray};
use arrow_cast::CastOptions;
use arrow_flight::sql::{client::FlightSqlServiceClient, CommandGetDbSchemas, CommandGetTables};
use arrow_flight::FlightInfo;
use arrow_schema::{DataType, Schema};
use error::MultibaseSnafu;
use feed_query::FeedQuery;
use flight_client::execute_flight_stream;
use napi::{bindgen_prelude::*, JsObject};
use napi_derive::{module_exports, napi};
use snafu::prelude::*;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::conversion::record_batches_to_buffer;
use crate::error::{ArrowSnafu, Result};
use crate::flight_client::{execute_flight, setup_client, ClientOptions};

fn init_logging() {
    // Set up a subscriber that logs to stdout
    if let Err(result) = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
    {
        warn!("Tracing already initialized: {}", result);
    }
}

#[module_exports]
fn init(_exports: JsObject) -> napi::Result<()> {
    init_logging();

    Ok(())
}

#[napi]
pub struct FlightSqlClient {
    client: Mutex<FlightSqlServiceClient<Channel>>,
}
impl FlightSqlClient {
    async fn query_flight_info(
        client: &mut FlightSqlServiceClient<Channel>,
        query: String,
    ) -> napi::Result<FlightInfo> {
        let flight_info = client.execute(query, None).await.context(ArrowSnafu {
            message: "failed to execute query",
        })?;
        Ok(flight_info)
    }
    async fn prepared_query_flight_info(
        client: &mut FlightSqlServiceClient<Channel>,
        query: String,
        params: Vec<(String, String)>,
    ) -> napi::Result<FlightInfo> {
        let mut prepared_stmt = client.prepare(query, None).await.context(ArrowSnafu {
            message: "failed to prepare statement",
        })?;
        let schema = prepared_stmt.parameter_schema().context(ArrowSnafu {
            message: "failed to retrieve parameter schema from prepare statement",
        })?;
        prepared_stmt
            .set_parameters(construct_record_batch_from_params(&params, schema)?)
            .context(ArrowSnafu {
                message: "failed to bind parameters",
            })?;
        let flight_info = prepared_stmt.execute().await.context(ArrowSnafu {
            message: "failed to execute prepared statement",
        })?;
        Ok(flight_info)
    }
}

#[napi]
impl FlightSqlClient {
    #[napi]
    pub async fn query(&self, query: String) -> napi::Result<Buffer> {
        let mut client = self.client.lock().await;

        let flight_info = Self::query_flight_info(&mut client, query).await?;

        let batches = execute_flight(&mut client, flight_info).await?;
        Ok(record_batches_to_buffer(batches)?.into())
    }

    #[napi]
    pub async fn feed_query(&self, query: String) -> napi::Result<FeedQuery> {
        let mut client = self.client.lock().await;

        let flight_info = Self::query_flight_info(&mut client, query).await?;

        let streams = execute_flight_stream(&mut client, flight_info).await?;
        Ok(FeedQuery::new(streams))
    }

    #[napi]
    pub async fn prepared_query(
        &self,
        query: String,
        params: Vec<(String, String)>,
    ) -> napi::Result<Buffer> {
        let mut client = self.client.lock().await;

        let flight_info = Self::prepared_query_flight_info(&mut client, query, params).await?;

        let batches = execute_flight(&mut client, flight_info).await?;
        Ok(record_batches_to_buffer(batches)?.into())
    }

    #[napi]
    pub async fn prepared_feed_query(
        &self,
        query: String,
        params: Vec<(String, String)>,
    ) -> napi::Result<FeedQuery> {
        let mut client = self.client.lock().await;

        let flight_info = Self::prepared_query_flight_info(&mut client, query, params).await?;

        let streams = execute_flight_stream(&mut client, flight_info).await?;
        Ok(FeedQuery::new(streams))
    }

    #[napi]
    pub async fn get_catalogs(&self) -> napi::Result<Buffer> {
        let mut client = self.client.lock().await;
        let flight_info = client.get_catalogs().await.context(ArrowSnafu {
            message: "failed to execute get catalogs",
        })?;
        let batches = execute_flight(&mut client, flight_info).await?;
        Ok(record_batches_to_buffer(batches)?.into())
    }

    #[napi]
    pub async fn get_db_schemas(&self, options: GetDbSchemasOptions) -> napi::Result<Buffer> {
        let command = CommandGetDbSchemas {
            catalog: options.catalog,
            db_schema_filter_pattern: options.db_schema_filter_pattern,
        };
        let mut client = self.client.lock().await;
        let flight_info = client.get_db_schemas(command).await.context(ArrowSnafu {
            message: "failed to execute get schemas",
        })?;
        let batches = execute_flight(&mut client, flight_info).await?;
        Ok(record_batches_to_buffer(batches)?.into())
    }

    #[napi]
    pub async fn get_tables(&self, options: GetTablesOptions) -> napi::Result<Buffer> {
        let command = CommandGetTables {
            catalog: options.catalog,
            db_schema_filter_pattern: options.db_schema_filter_pattern,
            table_name_filter_pattern: options.table_name_filter_pattern,
            table_types: options.table_types.unwrap_or_default(),
            include_schema: options.include_schema.unwrap_or_default(),
        };
        let mut client = self.client.lock().await;
        let flight_info = client.get_tables(command).await.context(ArrowSnafu {
            message: "failed to execute get tables",
        })?;
        let batches = execute_flight(&mut client, flight_info).await?;
        Ok(record_batches_to_buffer(batches)?.into())
    }
}

#[napi]
pub async fn create_flight_sql_client(
    options: ClientOptions,
) -> Result<FlightSqlClient, napi::Error> {
    Ok(FlightSqlClient {
        client: Mutex::new(setup_client(options).await.context(ArrowSnafu {
            message: "failed setting up flight sql client",
        })?),
    })
}

#[napi]
pub fn rust_crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[napi(object)]
pub struct GetDbSchemasOptions {
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    pub catalog: Option<String>,

    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    pub db_schema_filter_pattern: Option<String>,
}

#[napi(object)]
pub struct GetTablesOptions {
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    pub catalog: Option<String>,

    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    pub db_schema_filter_pattern: Option<String>,

    /// Specifies a filter pattern for tables to search for.
    /// When no table_name_filter_pattern is provided, all tables matching other filters are searched.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    pub table_name_filter_pattern: Option<String>,

    /// Specifies a filter of table types which must match.
    /// The table types depend on vendor/implementation.
    /// It is usually used to separate tables from views or system tables.
    /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
    pub table_types: Option<Vec<String>>,

    /// Specifies if the Arrow schema should be returned for found tables.
    pub include_schema: Option<bool>,
}

fn construct_record_batch_from_params(
    params: &[(String, String)],
    parameter_schema: &Schema,
) -> Result<RecordBatch> {
    let mut items = Vec::<(&String, ArrayRef)>::new();

    for (name, value) in params {
        let field = parameter_schema.field_with_name(name).context(ArrowSnafu {
            message: "failed to find field name in parameter schemas",
        })?;
        info!(name, field_type = ?field.data_type(), "parameter");
        if is_binary(field.data_type()) {
            let (_base, value) = multibase::decode(value).context(MultibaseSnafu {
                message: "binary parameter must be multibase encoded",
            })?;
            let value_as_array = BinaryArray::new_scalar(value);
            items.push((name, Arc::new(value_as_array.into_inner())))
        } else {
            let value_as_array = StringArray::new_scalar(value);
            let casted = arrow_cast::cast_with_options(
                value_as_array.get().0,
                field.data_type(),
                &CastOptions::default(),
            )
            .context(ArrowSnafu {
                message: "failed to cast parameter",
            })?;
            items.push((name, casted))
        }
    }

    RecordBatch::try_from_iter(items).context(ArrowSnafu {
        message: "failed to build record batch",
    })
}

fn is_binary(dt: &DataType) -> bool {
    match dt {
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => true,
        DataType::Dictionary(_, value_type) => is_binary(&value_type),
        _ => false,
    }
}
