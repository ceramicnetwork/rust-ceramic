use std::{io::Cursor, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_flight::{
    sql::{client::FlightSqlServiceClient, CommandGetDbSchemas, CommandGetTables},
    FlightInfo,
};
use clap::Subcommand;
use clap::{Args, ValueEnum};
use core::str;
use datafusion::arrow::{
    array::{ArrayRef, Datum as _, RecordBatch, StringArray},
    datatypes::Schema,
    util::pretty::pretty_format_batches,
};
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt as _;
use tonic::transport::{Channel, Endpoint};

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

    #[clap(subcommand)]
    cmd: Command,

    /// Output format of the results.
    #[arg(short, long, default_value = "table", env = "CERAMIC_ONE_QUERY_OUTPUT")]
    output: Output,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum Output {
    /// Output results in a human readable tabular form
    Table,
    /// Output results as a csv file with headers.
    Csv,
    /// Output results as new line delimited json objects.
    Json,
}

/// Different available commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Get catalogs.
    Catalogs,
    /// Get db schemas for a catalog.
    DbSchemas {
        /// Name of a catalog.
        ///
        /// Required.
        catalog: String,
        /// Specifies a filter pattern for schemas to search for.
        /// When no schema_filter is provided, the pattern will not be used to narrow the search.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        db_schema_filter: Option<String>,
    },
    /// Get tables for a catalog.
    Tables {
        /// Name of a catalog.
        ///
        /// Required.
        catalog: String,
        /// Specifies a filter pattern for schemas to search for.
        /// When no schema_filter is provided, the pattern will not be used to narrow the search.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        db_schema_filter: Option<String>,
        /// Specifies a filter pattern for tables to search for.
        /// When no table_filter is provided, all tables matching other filters are searched.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        table_filter: Option<String>,
        /// Specifies a filter of table types which must match.
        /// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
        /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
        #[clap(long)]
        table_types: Vec<String>,
    },
    /// Get table types.
    TableTypes,

    /// Execute given statement.
    StatementQuery {
        /// SQL query.
        ///
        /// Required.
        query: String,
    },

    /// Prepare given statement and then execute it.
    PreparedStatementQuery {
        /// SQL query.
        ///
        /// Required.
        ///
        /// Can contains placeholders like `$1`.
        ///
        /// Example: `SELECT * FROM t WHERE x = $1`
        query: String,

        /// Additional parameters.
        ///
        /// Can be given multiple times. Names and values are separated by '='. Values will be
        /// converted to the type that the server reported for the prepared statement.
        ///
        /// Example: `-p $1=42`
        #[clap(short, value_parser = parse_key_val)]
        params: Vec<(String, String)>,
    },
}

pub async fn run(opts: QueryOpts) -> Result<()> {
    let mut client = setup_client(opts.query_endpoint)
        .await
        .context("setup client")?;

    let flight_info = match opts.cmd {
        Command::Catalogs => client.get_catalogs().await.context("get catalogs")?,
        Command::DbSchemas {
            catalog,
            db_schema_filter,
        } => client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some(catalog),
                db_schema_filter_pattern: db_schema_filter,
            })
            .await
            .context("get db schemas")?,
        Command::Tables {
            catalog,
            db_schema_filter,
            table_filter,
            table_types,
        } => client
            .get_tables(CommandGetTables {
                catalog: Some(catalog),
                db_schema_filter_pattern: db_schema_filter,
                table_name_filter_pattern: table_filter,
                table_types,
                // Schema is returned as ipc encoded bytes.
                // We do not support returning the schema as there is no trivial mechanism
                // to display the information to the user.
                include_schema: false,
            })
            .await
            .context("get tables")?,
        Command::TableTypes => client.get_table_types().await.context("get table types")?,
        Command::StatementQuery { query } => client
            .execute(query, None)
            .await
            .context("execute statement")?,
        Command::PreparedStatementQuery { query, params } => {
            let mut prepared_stmt = client
                .prepare(query, None)
                .await
                .context("prepare statement")?;

            if !params.is_empty() {
                prepared_stmt
                    .set_parameters(
                        construct_record_batch_from_params(
                            &params,
                            prepared_stmt
                                .parameter_schema()
                                .context("get parameter schema")?,
                        )
                        .context("construct parameters")?,
                    )
                    .context("bind parameters")?;
            }

            prepared_stmt
                .execute()
                .await
                .context("execute prepared statement")?
        }
    };

    execute_flight(&mut client, flight_info, opts.output)
        .await
        .context("read flight data")?;

    Ok(())
}

async fn execute_flight(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
    output: Output,
) -> Result<()> {
    let schema = Arc::new(Schema::try_from(info.clone()).context("valid schema")?);
    let schema = RecordBatch::new_empty(schema);
    for endpoint in info.endpoint {
        let Some(ticket) = &endpoint.ticket else {
            bail!("did not get ticket");
        };

        let mut flight_data = client.do_get(ticket.clone()).await.context("do get")?;

        while let Some(data) = flight_data
            .try_next()
            .await
            .context("reading flight data")?
        {
            print_record_batches(output, &[schema.clone(), data])
                .await
                .context("print record batches")?;
        }
    }

    Ok(())
}

// Helper function to print a record batch
async fn print_record_batches(output: Output, batches: &[RecordBatch]) -> Result<()> {
    match output {
        Output::Table => {
            let res = pretty_format_batches(batches).context("format results")?;
            println!("{res}");
        }
        Output::Csv => {
            let mut buffer = Vec::new();
            let mut header = true;
            for batch in batches {
                {
                    let mut writer = arrow::csv::WriterBuilder::new()
                        .with_header(header)
                        .build(Cursor::new(&mut buffer));
                    writer.write(batch)?;
                    // Write the header only once
                    header = false;
                }
                tokio::io::stdout().write_all(&buffer).await?;
                buffer.clear();
            }
        }
        Output::Json => {
            let mut buffer = Vec::new();
            for batch in batches {
                {
                    let mut writer =
                        arrow::json::Writer::<_, arrow::json::writer::LineDelimited>::new(
                            Cursor::new(&mut buffer),
                        );
                    writer.write(batch)?;
                }
                tokio::io::stdout().write_all(&buffer).await?;
                buffer.clear();
            }
        }
    };
    Ok(())
}

fn construct_record_batch_from_params(
    params: &[(String, String)],
    parameter_schema: &Schema,
) -> Result<RecordBatch> {
    let mut items = Vec::<(&String, ArrayRef)>::new();

    for (name, value) in params {
        let field = parameter_schema.field_with_name(name)?;
        let value_as_array = StringArray::new_scalar(value);
        let casted = cast_with_options(
            value_as_array.get().0,
            field.data_type(),
            &CastOptions::default(),
        )?;
        items.push((name, casted))
    }

    Ok(RecordBatch::try_from_iter(items)?)
}

async fn setup_client(endpoint: String) -> Result<FlightSqlServiceClient<Channel>> {
    let endpoint = Endpoint::new(endpoint)
        .context("create endpoint")?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    let channel = endpoint.connect().await.context("connect to endpoint")?;

    Ok(FlightSqlServiceClient::new(channel))
}

/// Parse a single key-value pair
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].to_owned(), s[pos + 1..].to_owned()))
}
