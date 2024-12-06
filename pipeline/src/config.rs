use std::sync::Arc;

use object_store::ObjectStore;
use shutdown::Shutdown;

/// Configuration for pipeline session.
pub struct Config<F> {
    /// When true the aggregator actor is enabled.
    pub aggregator: bool,
    /// Define how the conclusion feed will be accessed.
    pub conclusion_feed: ConclusionFeedSource<F>,
    /// Access to an object store.
    pub object_store: Arc<dyn ObjectStore>,
    /// A shutdown signal channel.
    pub shutdown: Shutdown,
}

/// Define the source of the conclusion_feed table
pub enum ConclusionFeedSource<F> {
    /// Direct API access to the feed
    Direct(Arc<F>),
    /// Memory table
    #[cfg(test)]
    InMemory(datafusion::datasource::MemTable),
}

impl<F> From<F> for ConclusionFeedSource<F> {
    fn from(value: F) -> Self {
        Self::Direct(Arc::new(value))
    }
}
impl<F> From<&Arc<F>> for ConclusionFeedSource<F> {
    fn from(value: &Arc<F>) -> Self {
        Self::Direct(Arc::clone(value))
    }
}
