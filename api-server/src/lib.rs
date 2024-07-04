#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]
#![allow(unused_imports, unused_attributes)]
#![allow(clippy::derive_partial_eq_without_eq, clippy::disallowed_names)]

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::task::{Context, Poll};
use swagger::{ApiError, ContextWrapper};

type ServiceError = Box<dyn Error + Send + Sync + 'static>;

pub const BASE_PATH: &str = "/ceramic";
pub const API_VERSION: &str = "0.28.1";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DebugHeapGetResponse {
    /// success
    Success(swagger::ByteArray),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DebugHeapOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum EventsEventIdGetResponse {
    /// success
    Success(models::Event),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Event not found
    EventNotFound(String),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum EventsEventIdOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum EventsOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum EventsPostResponse {
    /// success
    Success,
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum ExperimentalEventsSepSepValueGetResponse {
    /// success
    Success(models::EventsGet),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ExperimentalEventsSepSepValueOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum ExperimentalInterestsGetResponse {
    /// success
    Success(models::InterestsGet),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ExperimentalInterestsOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum FeedEventsGetResponse {
    /// success
    Success(models::EventFeed),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum FeedEventsOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum FeedResumeTokenGetResponse {
    /// success
    Success(models::FeedResumeTokenGet200Response),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum FeedResumeTokenOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum InterestsOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum InterestsPostResponse {
    /// success
    Success,
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum InterestsSortKeySortValueOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum InterestsSortKeySortValuePostResponse {
    /// success
    Success,
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum LivenessGetResponse {
    /// success
    Success,
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum LivenessOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum VersionGetResponse {
    /// success
    Success(models::Version),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum VersionOptionsResponse {
    /// cors
    Cors,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum VersionPostResponse {
    /// success
    Success(models::Version),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
}

/// API
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait Api<C: Send + Sync> {
    fn poll_ready(
        &self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>> {
        Poll::Ready(Ok(()))
    }

    /// Get the heap statistics of the Ceramic node
    async fn debug_heap_get(&self, context: &C) -> Result<DebugHeapGetResponse, ApiError>;

    /// cors
    async fn debug_heap_options(&self, context: &C) -> Result<DebugHeapOptionsResponse, ApiError>;

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
        context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError>;

    /// cors
    async fn events_event_id_options(
        &self,
        event_id: String,
        context: &C,
    ) -> Result<EventsEventIdOptionsResponse, ApiError>;

    /// cors
    async fn events_options(&self, context: &C) -> Result<EventsOptionsResponse, ApiError>;

    /// Creates a new event
    async fn events_post(
        &self,
        event_data: models::EventData,
        context: &C,
    ) -> Result<EventsPostResponse, ApiError>;

    /// Get events matching the interest stored on the node
    async fn experimental_events_sep_sep_value_get(
        &self,
        sep: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ApiError>;

    /// cors
    async fn experimental_events_sep_sep_value_options(
        &self,
        sep: String,
        sep_value: String,
        context: &C,
    ) -> Result<ExperimentalEventsSepSepValueOptionsResponse, ApiError>;

    /// Get the interests stored on the node
    async fn experimental_interests_get(
        &self,
        context: &C,
    ) -> Result<ExperimentalInterestsGetResponse, ApiError>;

    /// cors
    async fn experimental_interests_options(
        &self,
        context: &C,
    ) -> Result<ExperimentalInterestsOptionsResponse, ApiError>;

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError>;

    /// cors
    async fn feed_events_options(&self, context: &C)
        -> Result<FeedEventsOptionsResponse, ApiError>;

    /// Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.
    async fn feed_resume_token_get(
        &self,
        context: &C,
    ) -> Result<FeedResumeTokenGetResponse, ApiError>;

    /// cors
    async fn feed_resume_token_options(
        &self,
        context: &C,
    ) -> Result<FeedResumeTokenOptionsResponse, ApiError>;

    /// cors
    async fn interests_options(&self, context: &C) -> Result<InterestsOptionsResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
        context: &C,
    ) -> Result<InterestsPostResponse, ApiError>;

    /// cors
    async fn interests_sort_key_sort_value_options(
        &self,
        sort_key: String,
        sort_value: String,
        context: &C,
    ) -> Result<InterestsSortKeySortValueOptionsResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_sort_key_sort_value_post(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        context: &C,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError>;

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self, context: &C) -> Result<LivenessGetResponse, ApiError>;

    /// cors
    async fn liveness_options(&self, context: &C) -> Result<LivenessOptionsResponse, ApiError>;

    /// Get the version of the Ceramic node
    async fn version_get(&self, context: &C) -> Result<VersionGetResponse, ApiError>;

    /// cors
    async fn version_options(&self, context: &C) -> Result<VersionOptionsResponse, ApiError>;

    /// Get the version of the Ceramic node
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError>;
}

/// API where `Context` isn't passed on every API call
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait ApiNoContext<C: Send + Sync> {
    fn poll_ready(
        &self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>>;

    fn context(&self) -> &C;

    /// Get the heap statistics of the Ceramic node
    async fn debug_heap_get(&self) -> Result<DebugHeapGetResponse, ApiError>;

    /// cors
    async fn debug_heap_options(&self) -> Result<DebugHeapOptionsResponse, ApiError>;

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ApiError>;

    /// cors
    async fn events_event_id_options(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdOptionsResponse, ApiError>;

    /// cors
    async fn events_options(&self) -> Result<EventsOptionsResponse, ApiError>;

    /// Creates a new event
    async fn events_post(
        &self,
        event_data: models::EventData,
    ) -> Result<EventsPostResponse, ApiError>;

    /// Get events matching the interest stored on the node
    async fn experimental_events_sep_sep_value_get(
        &self,
        sep: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ApiError>;

    /// cors
    async fn experimental_events_sep_sep_value_options(
        &self,
        sep: String,
        sep_value: String,
    ) -> Result<ExperimentalEventsSepSepValueOptionsResponse, ApiError>;

    /// Get the interests stored on the node
    async fn experimental_interests_get(
        &self,
    ) -> Result<ExperimentalInterestsGetResponse, ApiError>;

    /// cors
    async fn experimental_interests_options(
        &self,
    ) -> Result<ExperimentalInterestsOptionsResponse, ApiError>;

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
    ) -> Result<FeedEventsGetResponse, ApiError>;

    /// cors
    async fn feed_events_options(&self) -> Result<FeedEventsOptionsResponse, ApiError>;

    /// Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.
    async fn feed_resume_token_get(&self) -> Result<FeedResumeTokenGetResponse, ApiError>;

    /// cors
    async fn feed_resume_token_options(&self) -> Result<FeedResumeTokenOptionsResponse, ApiError>;

    /// cors
    async fn interests_options(&self) -> Result<InterestsOptionsResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
    ) -> Result<InterestsPostResponse, ApiError>;

    /// cors
    async fn interests_sort_key_sort_value_options(
        &self,
        sort_key: String,
        sort_value: String,
    ) -> Result<InterestsSortKeySortValueOptionsResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_sort_key_sort_value_post(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError>;

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self) -> Result<LivenessGetResponse, ApiError>;

    /// cors
    async fn liveness_options(&self) -> Result<LivenessOptionsResponse, ApiError>;

    /// Get the version of the Ceramic node
    async fn version_get(&self) -> Result<VersionGetResponse, ApiError>;

    /// cors
    async fn version_options(&self) -> Result<VersionOptionsResponse, ApiError>;

    /// Get the version of the Ceramic node
    async fn version_post(&self) -> Result<VersionPostResponse, ApiError>;
}

/// Trait to extend an API to make it easy to bind it to a context.
pub trait ContextWrapperExt<C: Send + Sync>
where
    Self: Sized,
{
    /// Binds this API to a context.
    fn with_context(self, context: C) -> ContextWrapper<Self, C>;
}

impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ContextWrapperExt<C> for T {
    fn with_context(self: T, context: C) -> ContextWrapper<T, C> {
        ContextWrapper::<T, C>::new(self, context)
    }
}

#[async_trait]
impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ApiNoContext<C> for ContextWrapper<T, C> {
    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), ServiceError>> {
        self.api().poll_ready(cx)
    }

    fn context(&self) -> &C {
        ContextWrapper::context(self)
    }

    /// Get the heap statistics of the Ceramic node
    async fn debug_heap_get(&self) -> Result<DebugHeapGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().debug_heap_get(&context).await
    }

    /// cors
    async fn debug_heap_options(&self) -> Result<DebugHeapOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().debug_heap_options(&context).await
    }

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_event_id_get(event_id, &context).await
    }

    /// cors
    async fn events_event_id_options(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_event_id_options(event_id, &context).await
    }

    /// cors
    async fn events_options(&self) -> Result<EventsOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_options(&context).await
    }

    /// Creates a new event
    async fn events_post(
        &self,
        event_data: models::EventData,
    ) -> Result<EventsPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_post(event_data, &context).await
    }

    /// Get events matching the interest stored on the node
    async fn experimental_events_sep_sep_value_get(
        &self,
        sep: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .experimental_events_sep_sep_value_get(
                sep, sep_value, controller, stream_id, offset, limit, &context,
            )
            .await
    }

    /// cors
    async fn experimental_events_sep_sep_value_options(
        &self,
        sep: String,
        sep_value: String,
    ) -> Result<ExperimentalEventsSepSepValueOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .experimental_events_sep_sep_value_options(sep, sep_value, &context)
            .await
    }

    /// Get the interests stored on the node
    async fn experimental_interests_get(
        &self,
    ) -> Result<ExperimentalInterestsGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().experimental_interests_get(&context).await
    }

    /// cors
    async fn experimental_interests_options(
        &self,
    ) -> Result<ExperimentalInterestsOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().experimental_interests_options(&context).await
    }

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
    ) -> Result<FeedEventsGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().feed_events_get(resume_at, limit, &context).await
    }

    /// cors
    async fn feed_events_options(&self) -> Result<FeedEventsOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().feed_events_options(&context).await
    }

    /// Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.
    async fn feed_resume_token_get(&self) -> Result<FeedResumeTokenGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().feed_resume_token_get(&context).await
    }

    /// cors
    async fn feed_resume_token_options(&self) -> Result<FeedResumeTokenOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().feed_resume_token_options(&context).await
    }

    /// cors
    async fn interests_options(&self) -> Result<InterestsOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().interests_options(&context).await
    }

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
    ) -> Result<InterestsPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().interests_post(interest, &context).await
    }

    /// cors
    async fn interests_sort_key_sort_value_options(
        &self,
        sort_key: String,
        sort_value: String,
    ) -> Result<InterestsSortKeySortValueOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .interests_sort_key_sort_value_options(sort_key, sort_value, &context)
            .await
    }

    /// Register interest for a sort key
    async fn interests_sort_key_sort_value_post(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .interests_sort_key_sort_value_post(
                sort_key, sort_value, controller, stream_id, &context,
            )
            .await
    }

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self) -> Result<LivenessGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().liveness_get(&context).await
    }

    /// cors
    async fn liveness_options(&self) -> Result<LivenessOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().liveness_options(&context).await
    }

    /// Get the version of the Ceramic node
    async fn version_get(&self) -> Result<VersionGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().version_get(&context).await
    }

    /// cors
    async fn version_options(&self) -> Result<VersionOptionsResponse, ApiError> {
        let context = self.context().clone();
        self.api().version_options(&context).await
    }

    /// Get the version of the Ceramic node
    async fn version_post(&self) -> Result<VersionPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().version_post(&context).await
    }
}

#[cfg(feature = "client")]
pub mod client;

// Re-export Client as a top-level name
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;

// Re-export router() as a top-level name
#[cfg(feature = "server")]
pub use self::server::Service;

#[cfg(feature = "server")]
pub mod context;

pub mod models;

#[cfg(any(feature = "client", feature = "server"))]
pub(crate) mod header;
