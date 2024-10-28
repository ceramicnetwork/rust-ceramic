use std::sync::Arc;

use object_store::ObjectStore;

/// Configuration for pipeline session.
pub struct Config<F> {
    /// Define how the conclusion feed will be accessed.
    pub conclusion_feed: Arc<F>,

    /// Bucket name in which to store objects.
    pub object_store_bucket_name: String,

    /// Access to an object store.
    pub object_store: Arc<dyn ObjectStore>,
}
