use arrow_flight::error::FlightError;
use arrow_schema::ArrowError;
use snafu::Snafu;
use tonic::Status;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("{message}: {source:?}"))]
    Multibase {
        source: multibase::Error,
        message: &'static str,
    },
    #[snafu(display("{message}: {source:?}"))]
    Arrow {
        source: ArrowError,
        message: &'static str,
    },
    #[snafu(display("{message}: {source:?}"))]
    Flight {
        source: FlightError,
        message: &'static str,
    },
    #[snafu(display("{message}: {source}"))]
    Status {
        source: Status,
        message: &'static str,
    },
}

impl From<Error> for napi::Error {
    fn from(value: Error) -> Self {
        napi::Error::from_reason(value.to_string())
    }
}
