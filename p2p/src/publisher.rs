use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::Result;
use ceramic_metrics::Recorder;
use futures_timer::Delay;
use futures_util::{future::BoxFuture, Future, Stream};
use libp2p::kad::{record::Key, AddProviderError, AddProviderOk};
use multihash::Multihash;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::{
    metrics::{self, Metrics},
    SQLiteBlockStore,
};

// Performing many queries concurrently is efficient for adjacent keys.
// We can use a large number here in order take advantage of those efficiencies.
const MAX_RUNNING_QUERIES: usize = 1000;
// Number of historical durations to track.
// Each batch tends to last up to the timeout because of the long tail of queries.
// As such there is not much variance in the batch duration and a small history is sufficient.
const ELAPSED_HISTORY_SIZE: usize = 10;
// Retries are relatively cheap and missing a publish can have network wide negative effects.
const MAX_RETRIES: usize = 10;
/// Scale between 0 and 1 of how optimistic we are.
/// A value of 0 means no optimism and therefore never delay the publisher loop.
/// A value of 1 means perfect optimism and therefore delay for all of our estimated ability.
const OPTIMISM: f64 = 0.5;

// Manages publishing provider records regularly over an interval.
pub struct Publisher {
    metrics: Metrics,
    state: State,
    interval: Duration,
    deadline: Instant,
    current_queries: HashSet<Key>,
    block_store: SQLiteBlockStore,
    last_hash: Option<Multihash>,
    batch_complete: Option<oneshot::Sender<()>>,
    elapsed_history: VecDeque<Duration>,
    retries: HashMap<Key, usize>,
}

enum State {
    StartingFetch,
    Delaying(Delay),
    FetchingBatch {
        start: Instant,
        future: BoxFuture<'static, Result<Batch>>,
    },
    WaitingOnBatch {
        start: Instant,
        remaining: i64,
        rx: oneshot::Receiver<()>,
    },
}

impl Publisher {
    pub fn new(interval: Duration, block_store: SQLiteBlockStore, metrics: Metrics) -> Self {
        Self {
            metrics,
            state: State::StartingFetch,
            deadline: Instant::now() + interval,
            interval,
            current_queries: HashSet::new(),
            block_store,
            last_hash: None,
            batch_complete: None,
            elapsed_history: VecDeque::with_capacity(ELAPSED_HISTORY_SIZE),
            retries: Default::default(),
        }
    }
    pub fn handle_start_providing_result(
        &mut self,
        result: Result<AddProviderOk, AddProviderError>,
    ) {
        let (key, metric_event) = match result {
            Ok(AddProviderOk { key }) => {
                self.retries.remove(&key);
                (
                    key,
                    Some(metrics::Event::PublishResult(
                        metrics::PublishResult::Success,
                    )),
                )
            }
            Err(AddProviderError::Timeout { key }) => {
                let metrics_event = if self.current_queries.contains(&key) {
                    let retry_count = self
                        .retries
                        .entry(key.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                    if *retry_count > MAX_RETRIES {
                        warn!(
                            key = hex::encode(key.to_vec()),
                            retries_attempted = MAX_RETRIES,
                            "kad: failed to provide record: timeout"
                        );
                        self.retries.remove(&key);
                        Some(metrics::Event::PublishResult(
                            metrics::PublishResult::Failed,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                };
                (key, metrics_event)
            }
        };
        self.metrics.record(&metric_event);
        self.current_queries.remove(&key);

        if self.current_queries.is_empty() {
            if let Some(tx) = self.batch_complete.take() {
                let _ = tx.send(());
            }
        }
    }
}

struct Batch {
    hashes: Vec<Multihash>,
    remaining: i64,
}

impl Stream for Publisher {
    type Item = Vec<Key>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Loop until we reach a blocking state.
        loop {
            match &mut self.state {
                State::Delaying(ref mut delay) => match Future::poll(Pin::new(delay), cx) {
                    Poll::Ready(_) => {
                        self.state = State::StartingFetch;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                State::StartingFetch => {
                    let block_store = self.block_store.clone();
                    let last_hash = self.last_hash;
                    let limit = (MAX_RUNNING_QUERIES - self.retries.len()) as i64;
                    self.state = State::FetchingBatch {
                        start: Instant::now(),
                        future: Box::pin(async move {
                            let (hashes, remaining) = block_store.range(last_hash, limit).await?;
                            Ok(Batch { hashes, remaining })
                        }),
                    };
                }
                State::FetchingBatch {
                    start,
                    ref mut future,
                } => {
                    match Future::poll(future.as_mut(), cx) {
                        Poll::Ready(Ok(batch)) => {
                            if batch.hashes.is_empty() {
                                let now = Instant::now();
                                debug!(
                                    deadline_seconds = self.deadline.duration_since(now).as_secs(),
                                    "no more blocks, delaying until deadline"
                                );
                                // We reached the end of the blocks.
                                // Delay until the deadline and reset it.
                                self.last_hash = None;
                                self.state = State::Delaying(Delay::new(self.deadline - now));
                                self.deadline = now + self.interval;
                            } else {
                                let start = *start;
                                self.last_hash = batch.hashes.iter().last().copied();
                                let (tx, rx) = oneshot::channel();
                                self.batch_complete = Some(tx);
                                self.state = State::WaitingOnBatch {
                                    start,
                                    remaining: batch.remaining,
                                    rx,
                                };

                                debug!(
                                    new_count = batch.hashes.len(),
                                    repeat_count = self.retries.len(),
                                    max_retry_count = self.retries.values().max().unwrap_or(&0),
                                    "starting new publish batch"
                                );
                                // Collect any keys that need to be retried and any new keys
                                let keys: Vec<Key> = self
                                    .retries
                                    .keys()
                                    .cloned()
                                    .chain(
                                        batch.hashes.into_iter().map(|hash| hash.to_bytes().into()),
                                    )
                                    .collect();
                                keys.iter().for_each(|key| {
                                    self.current_queries.insert(key.clone());
                                });
                                return Poll::Ready(Some(keys));
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            warn!(%err,"failed to fetch next batch of blocks to publish");
                            self.state = State::StartingFetch;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                State::WaitingOnBatch {
                    start,
                    remaining,
                    rx,
                } => {
                    match Future::poll(Pin::new(rx), cx) {
                        Poll::Ready(_) => {
                            let remaining_batches =
                                (*remaining as f64) / MAX_RUNNING_QUERIES as f64;
                            let elapsed = start.elapsed();
                            self.elapsed_history.push_front(elapsed);
                            if self.elapsed_history.len() >= ELAPSED_HISTORY_SIZE {
                                self.elapsed_history.pop_back();
                            }
                            let average = self
                                .elapsed_history
                                .iter()
                                .sum::<Duration>()
                                .div_f64(self.elapsed_history.len() as f64);
                            let needed = average.mul_f64(remaining_batches);
                            let now = Instant::now();
                            let estimated_finish = now + needed;
                            let spare = self.deadline.duration_since(estimated_finish);

                            // Compute useful diagnostic values
                            let needed_seconds = needed.as_secs_f64();
                            let deadline_seconds = self.deadline.duration_since(now).as_secs_f64();
                            let batch_average_seconds = average.as_secs();
                            let lag_ratio = needed_seconds / deadline_seconds;

                            if !spare.is_zero() {
                                // Be conservative and adjust our delay based on our optimism of
                                // the estimate.
                                let delay = spare.div_f64(remaining_batches / OPTIMISM);
                                debug!(
                                    batch_average_seconds,
                                    lag_ratio,
                                    delay_seconds = delay.as_secs(),
                                    "spare time, delaying, lag_ratio is (estimated needed time) / (remaining time before deadline)"
                                );
                                self.state = State::Delaying(Delay::new(delay));
                            } else {
                                warn!(
                                    batch_average_seconds,
                                    lag_ratio,
                                    "publisher has no spare time, lag_ratio is (estimated needed time) / (remaining time before deadline)"
                                );
                                self.state = State::StartingFetch;
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}
