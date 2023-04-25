//! Provides simple error type for communicating specific failures.
use thiserror::Error;

/// Error type for specific failures.
/// These generally come from here
/// <https://grpc.github.io/grpc/core/md_doc_statuscodes.html>
/// However we start with a minimal set and can add more as needed.
#[derive(Debug, Error)]
pub enum Error {
    /// Represents a resource was not found.
    #[error("not found")]
    NotFound,
    /// Represents a malformed request.
    /// Consumers need to fix their request.
    #[error("invalid: {0}")]
    Invalid(anyhow::Error),
    /// Represents a failure of the system,
    /// Consumers will likely have no control over fixing such an error.
    #[error("internal error: {0}")]
    Internal(anyhow::Error),
}
