//! Provides simple error type for communicating specific
//! HTTP status code along with errors.
use thiserror::Error;

/// Error type for HTTP responses.
///
/// Generally use InternalError for errors that
/// would be safe to retry once the internal issue is resolved.
/// Generally use BadRequest for errors that
/// would not be resolved with any amount of retries.
#[derive(Debug, Error)]
pub enum Error {
    /// Represents a failure of the system,
    /// clients will likely have no control over fixing such an error.
    #[error("internal error: {0}")]
    InternalError(anyhow::Error),
    /// Represents a malformed request.
    /// Client need to fix their request.
    #[error("bad request: {0}")]
    BadRequest(anyhow::Error),
    // Represents the resource was not found.
    #[error("not found")]
    NotFound,
}
