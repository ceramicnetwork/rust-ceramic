use anyhow::anyhow;

#[derive(Debug, thiserror::Error)]
/// The Errors that can be raised by store operations
pub enum StoreError {
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

impl StoreError {
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
            StoreError::Application { error } => Self::Application {
                error: error.context(context),
            },
            StoreError::Fatal { error } => Self::Fatal {
                error: error.context(context),
            },
            StoreError::Transient { error } => Self::Transient {
                error: error.context(context),
            },
        }
    }
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            // Transient errors can be retried after some time
            sqlx::Error::PoolTimedOut => {
                Self::new_transient(anyhow!("Timeout getting database connection"))
            }
            // typically transient, but could be fatal (e.g. OOM)
            sqlx::Error::Io(e) => Self::new_transient(e),

            // Unrecoverable (fatal) errors
            sqlx::Error::PoolClosed => Self::new_fatal(anyhow!("Database pool closed")), // Currently fatal, potentially recoverable in the future
            sqlx::Error::Configuration(e) => Self::new_fatal(anyhow!(e)),
            sqlx::Error::Protocol(e) => Self::new_fatal(anyhow!(e)),
            sqlx::Error::Tls(e) => Self::new_fatal(anyhow!(e)),
            sqlx::Error::WorkerCrashed => Self::new_fatal(anyhow!("Worker thread crashed")),
            // these column / decode errors mean our sql statements are invalid and the application can't resolve that
            sqlx::Error::ColumnIndexOutOfBounds { index, len } => Self::new_fatal(anyhow!(
                "Column index out of bounds: index {} out of {} columns",
                index,
                len
            )),
            sqlx::Error::ColumnNotFound(e) => Self::new_fatal(anyhow!(e)),
            sqlx::Error::ColumnDecode { index, source } => Self::new_fatal(anyhow!(
                "Column decode error: index '{}' - source: '{}'",
                index,
                source
            )),
            sqlx::Error::TypeNotFound { type_name } => {
                Self::new_fatal(anyhow!("Database type not found: {}", type_name))
            }
            sqlx::Error::AnyDriverError(e) => Self::new_fatal(anyhow!(e)),
            sqlx::Error::Migrate(e) => Self::new_fatal(e),

            // Application Errors (possible to retry with different input/fix/etc)
            sqlx::Error::Database(e) => Self::new_app(e),
            sqlx::Error::Decode(e) => Self::new_app(anyhow!(e)),
            sqlx::Error::RowNotFound => Self::new_app(anyhow!("Row not found")),
            // non_exhaustive
            // TODO: is there a way to skip a variant and throw a compilation error if one is ever added?
            e => Self::new_app(e),
        }
    }
}
