use anyhow::anyhow;

#[derive(Debug, thiserror::Error)]
/// The Errors that can be raised by store operations
pub enum Error {
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
}

impl Error {
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
            Error::Application { error } => Self::Application {
                error: error.context(context),
            },
            Error::Fatal { error } => Self::Fatal {
                error: error.context(context),
            },
            Error::Transient { error } => Self::Transient {
                error: error.context(context),
            },
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::new_fatal(anyhow!(value.to_string()).context("Unable to send to recon server"))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::new_fatal(anyhow!(value.to_string()).context("No response from recon server"))
    }
}
