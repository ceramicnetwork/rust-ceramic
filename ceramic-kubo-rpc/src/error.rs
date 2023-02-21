//! Provides simple error type for communicating specific
//! HTTP status code along with errors.
use actix_web::{
    error,
    http::{header::ContentType, StatusCode},
    HttpResponse,
};
use serde::Serialize;
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

#[derive(Serialize)]
struct ErrorJson<'a> {
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Code")]
    pub code: i32,
    #[serde(rename = "Type")]
    pub typ: &'a str,
}

impl error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        let err = ErrorJson {
            message: self.to_string(),
            code: 0,
            typ: "error",
        };
        let data = serde_json::to_string(&err).unwrap();
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .body(data)
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
        }
    }
}
