//! Pipeline provides a set of tables of Ceramic events and transformations between them.

pub mod aggregator;
mod cache_table;
pub mod cid_string;
mod conclusion;
#[warn(missing_docs)]
mod config;
pub mod schemas;

#[cfg(test)]
pub mod tests;

use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use cache_table::CacheTable;
use datafusion::{
    catalog_common::MemorySchemaProvider,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::{config::SessionConfig, context::SessionContext},
    functions_aggregate::first_last::LastValue,
    logical_expr::{col, AggregateUDF, ScalarUDF},
};
use schemas::doc_state;
use url::Url;

use cid_string::{CidString, CidStringList};

pub use conclusion::{
    conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionFeed,
    ConclusionInit, ConclusionTime,
};
pub use config::{ConclusionFeedSource, Config};

pub const CONCLUSION_FEED_TABLE: &str = "ceramic.v0.conclusion_feed";
pub const DOC_STATE_TABLE: &str = "ceramic.v0.doc_state";
pub const DOC_STATE_MEM_TABLE: &str = "ceramic._internal.doc_state_mem";
pub const DOC_STATE_PERSISTENT_TABLE: &str = "ceramic._internal.doc_state_persistent";

/// Constructs a [`SessionContext`] configured with all tables in the pipeline.
pub async fn session_from_config<F: ConclusionFeed + 'static>(
    config: impl Into<Config<F>>,
) -> Result<SessionContext> {
    let config: Config<F> = config.into();

    let session_config = SessionConfig::new()
        .with_default_catalog_and_schema("ceramic", "v0")
        .with_information_schema(true);

    let mut ctx = SessionContext::new_with_config(session_config);
    match config.conclusion_feed {
        ConclusionFeedSource::Direct(conclusion_feed) => {
            ctx.register_table(
                CONCLUSION_FEED_TABLE,
                Arc::new(conclusion::FeedTable::new(conclusion_feed)),
            )?;
        }
        #[cfg(test)]
        ConclusionFeedSource::InMemory(table) => {
            assert_eq!(
                schemas::conclusion_feed(),
                datafusion::catalog::TableProvider::schema(&table)
            );
            ctx.register_table("conclusion_feed", Arc::new(table))?;
        }
    };
    // Register the _internal schema
    ctx.catalog("ceramic")
        .expect("ceramic catalog should always exist")
        .register_schema("_internal", Arc::new(MemorySchemaProvider::default()))?;

    // Register various UDxFs
    ctx.register_udaf(AggregateUDF::new_from_impl(LastValue::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidStringList::new()));

    // Register JSON functions
    datafusion_functions_json::register_all(&mut ctx)?;

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
    let doc_state_object_store_path = DOC_STATE_TABLE.replace('.', "/") + "/";
    url.set_path(&doc_state_object_store_path);
    // Register doc_state_persistent as a listing table
    ctx.register_table(
        DOC_STATE_PERSISTENT_TABLE,
        Arc::new(ListingTable::try_new(
            ListingTableConfig::new(ListingTableUrl::parse(url)?)
                .with_listing_options(listing_options)
                .with_schema(schemas::doc_state()),
        )?),
    )?;

    ctx.register_table(
        DOC_STATE_MEM_TABLE,
        Arc::new(CacheTable::try_new(
            doc_state(),
            vec![vec![RecordBatch::new_empty(doc_state())]],
        )?),
    )?;

    ctx.register_table(
        DOC_STATE_TABLE,
        ctx.table(DOC_STATE_MEM_TABLE)
            .await?
            .union(ctx.table(DOC_STATE_PERSISTENT_TABLE).await?)?
            .into_view(),
    )?;

    Ok(ctx)
}
