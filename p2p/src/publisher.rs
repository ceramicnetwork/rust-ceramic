use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
    vec::IntoIter,
};

use anyhow::Result;
use ceramic_metrics::Recorder;
use futures_timer::Delay;
use futures_util::{future::BoxFuture, Future, Stream};
use libp2p::kad::{AddProviderError, AddProviderOk, AddProviderResult, RecordKey};
use multihash::Multihash;
use tokio::sync::{
    mpsc::{channel, error::TrySendError, Receiver, Sender},
    oneshot,
};
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::{
    metrics::{self, Metrics},
    SQLiteBlockStore,
};

// Retries are relatively cheap and missing a publish can have network wide negative effects.
const MAX_RETRIES: usize = 10;
// Initial batch size, choose a conservative value and grow it as we are successful.
const BATCH_INITIAL_SIZE: usize = 500;
// Maximum batch size.
const BATCH_MAXIMUM_SIZE: usize = 3000;
// Increase the batch size if the success ratio is greater than this threshold.
// Otherwise decrease the batch size.
const BATCH_THRESHOLD: f64 = 0.99;
// Coefficient used to decrease batch size when a batch failure is detected.
const BATCH_MULTIPLICATIVE_DECREASE: f64 = 0.5;
// Term used to increase batch size when a batch success is detected.
const BATCH_ADDITIVE_INCREASE: usize = 100;

// Manages publishing provider records regularly over an interval.
// Publisher implements [`Stream`] to produce batches of DHT keys to provide.
pub struct Publisher {
    start_providing_results_tx: Sender<AddProviderResult>,
    batch: Option<IntoIter<RecordKey>>,
    batches_rx: Receiver<Vec<RecordKey>>,
    metrics: Metrics,
}

impl Publisher {
    pub fn new(interval: Duration, block_store: SQLiteBlockStore, metrics: Metrics) -> Self {
        // Channel for result of each start_provide query.
        let (results_tx, results_rx) = channel(BATCH_MAXIMUM_SIZE);

        // We should rarely be behind by more than a single batch.
        // If we are, back-pressure is good as we cannot use the new batch.
        let (batches_tx, batches_rx) = channel(1);

        // Do real work of the publisher on its own task.
        let mut stream = PublisherWorker::new(results_rx, interval, block_store, metrics.clone());
        let task_metrics = metrics.clone();
        tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                if batches_tx.send(batch).await.is_err() {
                    error!("failed to send batch on channel");
                    task_metrics.record(&metrics::PublisherEvent::BatchSendErr);
                }
            }
        });

        Self {
            batch: None,
            batches_rx,
            start_providing_results_tx: results_tx,
            metrics,
        }
    }
    pub fn handle_start_providing_result(&mut self, result: AddProviderResult) {
        if let Err(send_err) = self.start_providing_results_tx.try_send(result) {
            self.metrics.record(&metrics::PublisherEvent::BatchSendErr);
            match send_err {
                TrySendError::Full(result) => {
                    error!(
                        ?result,
                        "failed to send start providing result on channel; channel is full"
                    )
                }
                TrySendError::Closed(result) => {
                    error!(
                        ?result,
                        "failed to send start providing result on channel; channel is closed"
                    )
                }
            };
        }
    }
}

// This implementation needs to be light, as it shares its task with the swarm.
// As such we use an internal channel and offload the real work to a separate task.
//
// This stream produces one key at a time. This way we do not flood the swarm task with lots of
// work.
impl Stream for Publisher {
    type Item = RecordKey;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(mut batch) = self.batch.take() {
                if let Some(key) = batch.next() {
                    self.batch = Some(batch);
                    return Poll::Ready(Some(key));
                } // else drop the batch because it is empty
            }

            match self.batches_rx.poll_recv(cx) {
                Poll::Ready(Some(batch)) => {
                    self.batch = Some(batch.into_iter());
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// Manages retrieving block hashes from the SQLiteBlockStore and producing a stream of record keys
// to publish.
struct PublisherWorker {
    metrics: Metrics,
    state: State,
    interval: Duration,
    deadline: Instant,
    batch_size: usize,
    results_rx: Receiver<AddProviderResult>,
    current_queries: HashSet<RecordKey>,
    block_store: SQLiteBlockStore,
    last_hash: Option<Multihash>,
    batch_complete: Option<oneshot::Sender<()>>,
    retries: HashMap<RecordKey, usize>,
}

enum State {
    StartingFetch,
    Delaying(Delay),
    FetchingBatch {
        future: BoxFuture<'static, Result<Batch>>,
    },
    StartingBatch {
        batch: Batch,
    },
    WaitingOnBatch {
        remaining: Option<i64>,
        rx: oneshot::Receiver<()>,
    },
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartingFetch => write!(f, "StartingFetch"),
            Self::Delaying(arg0) => f.debug_tuple("Delaying").field(arg0).finish(),
            Self::FetchingBatch { future: _ } => f.debug_struct("FetchingBatch").finish(),
            Self::StartingBatch { batch } => f
                .debug_struct("StartingBatch")
                .field("batch", batch)
                .finish(),
            Self::WaitingOnBatch { remaining, rx } => f
                .debug_struct("WaitingOnBatch")
                .field("remaining", remaining)
                .field("rx", rx)
                .finish(),
        }
    }
}

impl PublisherWorker {
    pub fn new(
        results_rx: Receiver<AddProviderResult>,
        interval: Duration,
        block_store: SQLiteBlockStore,
        metrics: Metrics,
    ) -> Self {
        Self {
            metrics,
            state: State::StartingFetch,
            deadline: Instant::now() + interval,
            interval,
            batch_size: BATCH_INITIAL_SIZE,
            results_rx,
            current_queries: HashSet::new(),
            block_store,
            last_hash: None,
            batch_complete: None,
            retries: Default::default(),
        }
    }
    fn handle_publish_result(&mut self, result: AddProviderResult) {
        let (key, metric_event) = match result {
            Ok(AddProviderOk { key }) => {
                self.retries.remove(&key);
                (
                    key,
                    Some(metrics::PublisherEvent::Result(
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
                        Some(metrics::PublisherEvent::Result(
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
        if let Some(event) = metric_event {
            self.metrics.record(&event);
        }
        self.current_queries.remove(&key);

        if self.current_queries.is_empty() {
            if let Some(tx) = self.batch_complete.take() {
                let _ = tx.send(());
            }
        }
    }
}

#[derive(Debug)]
struct Batch {
    hashes: Vec<Multihash>,
    remaining: Option<i64>,
}

impl Stream for PublisherWorker {
    type Item = Vec<RecordKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        trace!(state=?self.state, "poll_next");
        // First process any results.
        loop {
            match self.results_rx.poll_recv(cx) {
                Poll::Ready(Some(result)) => {
                    self.handle_publish_result(result);
                }
                Poll::Ready(None) => {
                    warn!("results channel is closed");
                    // We cannot continue to process batches so indicate the stream is complete.
                    return Poll::Ready(None);
                }
                Poll::Pending => break,
            };
        }
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
                    if let Some(limit) = self.batch_size.checked_sub(self.retries.len()) {
                        self.state = State::FetchingBatch {
                            future: Box::pin(async move {
                                let (hashes, remaining) =
                                    block_store.range(last_hash, limit as i64).await?;
                                Ok(Batch {
                                    hashes,
                                    remaining: Some(remaining),
                                })
                            }),
                        };
                    } else {
                        self.state = State::StartingBatch {
                            batch: Batch {
                                hashes: Default::default(),
                                remaining: None,
                            },
                        };
                    }
                }
                State::FetchingBatch { ref mut future } => {
                    match Future::poll(future.as_mut(), cx) {
                        Poll::Ready(Ok(batch)) => {
                            if batch.hashes.is_empty() {
                                let now = Instant::now();
                                let delay = self.deadline.checked_duration_since(now);
                                debug!(
                                    deadline_seconds =
                                        delay.map(|d| d.as_secs()).unwrap_or_default(),
                                    "no more blocks, delaying until deadline"
                                );
                                // We reached the end of the blocks.
                                // Delay until the deadline and reset it.
                                self.last_hash = None;
                                if let Some(delay) = self.deadline.checked_duration_since(now) {
                                    self.state = State::Delaying(Delay::new(delay));
                                } else {
                                    // We were behind schedule, start new batch immediately.
                                    self.state = State::StartingFetch
                                };
                                self.deadline = now + self.interval;
                            } else {
                                self.state = State::StartingBatch { batch };
                            }
                        }
                        Poll::Ready(Err(err)) => {
                            warn!(%err,"failed to fetch next batch of blocks to publish");
                            self.state = State::StartingFetch;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                State::StartingBatch { batch } => {
                    let last_hash = batch.hashes.iter().last().copied();
                    let remaining = batch.remaining;
                    let new_count = batch.hashes.len() as i64;

                    // Collect the new keys
                    let new_keys: Vec<RecordKey> = batch
                        .hashes
                        .iter()
                        .map(|hash| hash.to_bytes().into())
                        .collect();

                    // Collect all keys including the keys that need to be retried up to the batch size limit.
                    // We limit the number of new keys to ensure we have room for retries.
                    // However if the batch size is smaller than the number of retries we need to
                    // only retry up to batch size.
                    let mut max_retry_count = 0i64;
                    let mut repeat_count = 0;
                    let keys: Vec<RecordKey> = self
                        .retries
                        .iter()
                        .take(self.batch_size - new_keys.len())
                        // Use inspect to track stats about the retries
                        .inspect(|(_key, retry_count)| {
                            max_retry_count = max(max_retry_count, **retry_count as i64);
                            repeat_count += 1;
                        })
                        .map(|(key, _)| key.clone())
                        .chain(new_keys)
                        .collect();

                    // Expect a response for each key.
                    // Current queries should be empty because we finished the last batch.
                    debug_assert!(self.current_queries.is_empty());
                    keys.iter().for_each(|key| {
                        self.current_queries.insert(key.clone());
                    });

                    // Setup notification channel for when batch is complete
                    let (tx, rx) = oneshot::channel();

                    let retry_queue_len = self.retries.len();
                    // Record logs and metrics
                    debug!(
                        new_count,
                        repeat_count,
                        max_retry_count,
                        retry_queue_len,
                        "starting new publish batch"
                    );
                    self.metrics.record(&metrics::PublisherEvent::BatchStarted {
                        new_count,
                        repeat_count,
                        max_retry_count,
                    });
                    // Update state
                    self.last_hash = last_hash;
                    self.batch_complete = Some(tx);
                    self.state = State::WaitingOnBatch { remaining, rx };

                    // Report batch keys are ready to be published
                    return Poll::Ready(Some(keys));
                }
                State::WaitingOnBatch { remaining, rx } => {
                    match Future::poll(Pin::new(rx), cx) {
                        Poll::Ready(_) => {
                            let remaining = *remaining;
                            let batch_size = self.batch_size;
                            let deadline_seconds =
                                self.deadline.duration_since(Instant::now()).as_secs() as i64;

                            // Update next batch_size using additive increase or multiplicative decrease.
                            // This means we do not publish too fast, instead if we see failures we
                            // slow down the rate at which we publish.
                            let success_ratio =
                                ((batch_size - self.retries.len()) as f64) / (batch_size as f64);
                            if success_ratio > BATCH_THRESHOLD {
                                // Batch succeeded, increase batch size
                                self.batch_size += BATCH_ADDITIVE_INCREASE;
                            } else {
                                // Batch failed, decrease batch size
                                self.batch_size = (self.batch_size as f64
                                    * BATCH_MULTIPLICATIVE_DECREASE)
                                    as usize;
                            };
                            // Always use a batch size between [`BATCH_ADDITIVE_INCREASE`] and
                            // [`BATCH_MAXIMUM_SIZE`].
                            self.batch_size = max(BATCH_ADDITIVE_INCREASE, self.batch_size);
                            self.batch_size = min(BATCH_MAXIMUM_SIZE, self.batch_size);

                            debug!(
                                success_ratio,
                                batch_size,
                                next_batch_size = self.batch_size,
                                remaining,
                                deadline_seconds,
                                "batch finished"
                            );
                            self.metrics
                                .record(&metrics::PublisherEvent::BatchFinished {
                                    batch_size: batch_size as i64,
                                    remaining,
                                    deadline_seconds,
                                });

                            // Fetch next batch
                            self.state = State::StartingFetch;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}
