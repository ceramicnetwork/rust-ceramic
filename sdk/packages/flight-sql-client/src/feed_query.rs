use arrow_flight::decode::FlightRecordBatchStream;
use futures::TryStreamExt as _;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use snafu::ResultExt as _;
use tokio::sync::Mutex;

use crate::{conversion::record_batch_to_buffer, error::FlightSnafu};

#[napi]
pub struct FeedQuery {
    streams: Mutex<Vec<FlightRecordBatchStream>>,
}

#[napi]
impl FeedQuery {
    pub fn new(streams: Vec<FlightRecordBatchStream>) -> Self {
        Self {
            streams: Mutex::new(streams),
        }
    }

    #[napi]
    pub async fn next(&self) -> napi::Result<Option<Buffer>> {
        let mut streams = self.streams.lock().await;
        while let Some(mut stream) = streams.pop() {
            let batch = stream.try_next().await.context(FlightSnafu {
                message: "failed to read next batch from server",
            })?;
            if let Some(batch) = batch {
                // This stream may have more data push it back onto the stack
                streams.push(stream);
                return Ok(Some(record_batch_to_buffer(batch)?.into()));
            } // else the stream is empty, get the next one
        }
        // All streams are finished, we are finished.
        Ok(None)
    }
}
