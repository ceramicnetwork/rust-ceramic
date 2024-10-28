//! Pipeline provides a set of tables of Ceramic events and transformations between them.

pub mod aggregator;
pub mod cid_string;
mod conclusion;
#[warn(missing_docs)]
mod config;
pub mod schemas;

use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    execution::{config::SessionConfig, context::SessionContext},
    functions_aggregate::first_last::LastValue,
    logical_expr::{col, AggregateUDF, ScalarUDF},
};
use url::Url;

use cid_string::{CidString, CidStringList};

pub use conclusion::{
    conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionFeed,
    ConclusionInit, ConclusionTime,
};
pub use config::Config;

/// Constructs a [`SessionContext`] configured with all tables in the pipeline.
pub async fn session_from_config<F: ConclusionFeed + 'static>(
    config: impl Into<Config<F>>,
) -> Result<SessionContext> {
    let config: Config<F> = config.into();

    let session_config = SessionConfig::new().with_default_catalog_and_schema("ceramic", "v0");

    let ctx = SessionContext::new_with_config(session_config);
    ctx.register_table(
        "conclusion_feed",
        Arc::new(conclusion::FeedTable::new(config.conclusion_feed)),
    )?;

    // Register various UDxFs
    ctx.register_udaf(AggregateUDF::new_from_impl(LastValue::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidStringList::new()));

    // Register s3 object store
    let mut url = Url::parse("s3://")?;
    url.set_host(Some(&config.object_store_bucket_name))?;
    ctx.register_object_store(&url, config.object_store);

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
