#[derive(Debug, thiserror::Error)]
/// The Errors that can be raised by store operations
pub enum Error {
    #[error("Application error encountered: {error}")]
    /// An internal application error that is not fatal to the process e.g. a 500/unhandled error
    Application {
        /// The error details that may include context and other information
        error: anyhow::Error,
    },
    #[error("InvalidArgument: {error}")]
    /// Invalid client input
    InvalidArgument {
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

    /// Crate an InvalidArgument error
    pub fn new_invalid_arg(error: impl Into<anyhow::Error>) -> Self {
        Self::InvalidArgument {
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
            Error::InvalidArgument { error } => Self::InvalidArgument {
                error: error.context(context),
            },
        }
    }
}

impl From<Error> for recon::Error {
    fn from(value: Error) -> Self {
        match value {
            Error::Application { error } => recon::Error::Application { error },
            Error::Fatal { error } => recon::Error::Fatal { error },
            Error::Transient { error } => recon::Error::Transient { error },
            Error::InvalidArgument { error } => recon::Error::Application { error },
        }
    }
}

impl From<ceramic_store::Error> for Error {
    fn from(value: ceramic_store::Error) -> Self {
        match value {
            ceramic_store::Error::Application { error } => Error::Application { error },
            ceramic_store::Error::Fatal { error } => Error::Fatal { error },
            ceramic_store::Error::Transient { error } => Error::Transient { error },
            ceramic_store::Error::InvalidArgument { error } => Error::InvalidArgument { error },
        }
    }
}
