use anyhow::anyhow;

#[derive(Debug, thiserror::Error)]
/// The Errors that can be raised by store operations
pub enum ReconError {
    #[error("Application error encountered: {error}")]
    /// An application specific error that may be resolved with different inputs or other changes
    Application {
        /// The error details that may include context and other information
        error: anyhow::Error,
    },
    #[error("Fatal error encountered: {error}")]
    /// A fatal error that is unlikely to be recoverable, and may require terminating the process completely
    Fatal {
        /// The error details that may include context and other information
        error: anyhow::Error,
    },
    #[error("Transient error encountered: {error}")]
    /// An error that can be retried, and may resolve itself. If an error is transient repeatedly, it should be
    /// considered an "application" level error and propagated upward.
    Transient {
        /// The error details that may include context and other information
        error: anyhow::Error,
    },

    #[error("Worker died: {error}")]
    /// The worker task died, usually the result of a channel being closed unexpectedly.
    WorkerDied {
        /// The error details that may include context and other information
        error: anyhow::Error,
    },
}

impl ReconError {
    /// Create a transient error
    pub fn new_transient(error: impl Into<anyhow::Error>) -> Self {
        Self::Transient {
            error: error.into(),
        }
    }
    /// Create a fatal error
    pub fn new_fatal(error: impl Into<anyhow::Error>) -> Self {
        Self::Fatal {
            error: error.into(),
        }
    }

    /// Create an application error
    pub fn new_app(error: impl Into<anyhow::Error>) -> Self {
        Self::Application {
            error: error.into(),
        }
    }

    /// Add context to the internal error. Works identically to `anyhow::context`
    pub fn context<C>(self, context: C) -> Self
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        match self {
            ReconError::Application { error } => Self::Application {
                error: error.context(context),
            },
            ReconError::Fatal { error } => Self::Fatal {
                error: error.context(context),
            },
            ReconError::Transient { error } => Self::Transient {
                error: error.context(context),
            },
            ReconError::WorkerDied { error } => Self::WorkerDied {
                error: error.context(context),
            },
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ReconError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::WorkerDied {
            error: anyhow!(value.to_string()),
        }
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for ReconError {
    fn from(value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::WorkerDied {
            error: anyhow!(value.to_string()),
        }
    }
}
