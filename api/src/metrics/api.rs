use async_trait::async_trait;
use ceramic_api_server::{
    models, Api, EventsEventIdGetResponse, EventsPostResponse, EventsSortKeySortValueGetResponse,
    FeedEventsGetResponse, InterestsSortKeySortValuePostResponse, LivenessGetResponse,
    VersionPostResponse,
};
use ceramic_metrics::Recorder;
use futures::Future;
use swagger::ApiError;
use tokio::time::Instant;

use crate::{metrics::Event, Metrics};

/// Implement the API and record metrics
#[derive(Clone)]
pub struct MetricsMiddleware<A: Clone> {
    api: A,
    metrics: Metrics,
}

impl<A: Clone> MetricsMiddleware<A> {
    /// Construct a new MetricsMiddleware.
    /// The metrics should have already be registered.
    pub fn new(api: A, metrics: Metrics) -> Self {
        Self { api, metrics }
    }
    // Record metrics for a given API endpoint
    async fn record<T>(&self, path: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = Event { path, duration };
        self.metrics.record(&event);
        ret
    }
}

#[async_trait]
impl<A, C> Api<C> for MetricsMiddleware<A>
where
    A: Api<C>,
    A: Clone + Send + Sync,
    C: Send + Sync,
{
    /// Creates a new event
    async fn events_post(
        &self,
        event: models::Event,
        context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        self.record("/events", self.api.events_post(event, context))
            .await
    }

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self, context: &C) -> Result<LivenessGetResponse, ApiError> {
        self.record("/liveness", self.api.liveness_get(context))
            .await
    }

    /// Get event data
    async fn feed_events_get(
        &self,
        param_resume_at: Option<String>,
        param_limit: Option<i32>,
        context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError> {
        self.record(
            "/feed/events",
            self.api
                .feed_events_get(param_resume_at, param_limit, context),
        )
        .await
    }

    /// Get events for a stream
    async fn events_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<EventsSortKeySortValueGetResponse, ApiError> {
        self.record(
            "/events",
            self.api.events_sort_key_sort_value_get(
                sort_key, sort_value, controller, stream_id, offset, limit, context,
            ),
        )
        .await
    }

    /// Get the version of the Ceramic node
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError> {
        self.record("/version", self.api.version_post(context))
            .await
    }

    /// Register interest for a sort key
    async fn interests_sort_key_sort_value_post(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        context: &C,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError> {
        self.record(
            "/interests",
            self.api.interests_sort_key_sort_value_post(
                sort_key, sort_value, controller, stream_id, context,
            ),
        )
        .await
    }

    async fn events_event_id_get(
        &self,
        event_id: String,
        context: &C,
    ) -> std::result::Result<EventsEventIdGetResponse, ApiError> {
        self.record(
            "/events/{event_id}",
            self.api.events_event_id_get(event_id, context),
        )
        .await
    }
}
