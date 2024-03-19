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
pub const API_VERSION: &str = "0.13.0";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum EventsEventIdGetResponse {
    /// success
    Success(models::Event),
    /// Event not found
    EventNotFound(String),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
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
pub enum EventsSortKeySortValueGetResponse {
    /// success
    Success(models::EventsGet),
    /// bad request
    BadRequest(models::BadRequestResponse),
    /// Internal server error
    InternalServerError(models::ErrorResponse),
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

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
        context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError>;

    /// Creates a new event
    async fn events_post(
        &self,
        event: models::Event,
        context: &C,
    ) -> Result<EventsPostResponse, ApiError>;

    /// Get events matching the interest stored on the node
    async fn events_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<EventsSortKeySortValueGetResponse, ApiError>;

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
        context: &C,
    ) -> Result<InterestsPostResponse, ApiError>;

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

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ApiError>;

    /// Creates a new event
    async fn events_post(&self, event: models::Event) -> Result<EventsPostResponse, ApiError>;

    /// Get events matching the interest stored on the node
    async fn events_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<EventsSortKeySortValueGetResponse, ApiError>;

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
    ) -> Result<FeedEventsGetResponse, ApiError>;

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
    ) -> Result<InterestsPostResponse, ApiError>;

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

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_event_id_get(event_id, &context).await
    }

    /// Creates a new event
    async fn events_post(&self, event: models::Event) -> Result<EventsPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().events_post(event, &context).await
    }

    /// Get events matching the interest stored on the node
    async fn events_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<EventsSortKeySortValueGetResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .events_sort_key_sort_value_get(
                sort_key, sort_value, controller, stream_id, offset, limit, &context,
            )
            .await
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

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
    ) -> Result<InterestsPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().interests_post(interest, &context).await
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
