use async_trait::async_trait;
use ceramic_pipeline::{ConclusionEvent, ConclusionFeed};
use futures::future::try_join_all;

use crate::EventService;

#[async_trait]
impl ConclusionFeed for EventService {
    /// Fetches Conclusion Events that have occurred since a given highwater mark.
    ///
    /// This function retrieves events that have been processed after the specified highwater mark,
    /// up to the specified limit. It transforms raw events into `ConclusionEvent` structures,
    /// which provide a standardized format for event data.
    ///
    /// # Arguments
    ///
    /// * `highwater_mark` - An `i64` representing the starting point from which to fetch events.
    ///   Events with a higher mark than this value will be returned.
    /// * `limit` - An `i64` specifying the maximum number of events to return.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Vec<ConclusionEvent>` if successful, or an `anyhow::Error` if an error occurs.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - There's a problem fetching events from the database
    /// - There's an issue transforming raw events into `ConclusionEvent` structures
    async fn conclusion_events_since(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ConclusionEvent>> {
        let raw_events = self
            // TODO: Can we make highwater_marks zero based?
            // highwater marks are 1 based, add one
            .fetch_events_since_highwater_mark(highwater_mark + 1, limit)
            .await?;

        let conclusion_events_futures = raw_events
            .into_iter()
            .map(|event| self.transform_raw_events_to_conclusion_events(event));

        try_join_all(conclusion_events_futures)
            .await
            .map_err(Into::into)
    }
}
