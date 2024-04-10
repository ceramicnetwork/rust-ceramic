use anyhow::Result;
use ceramic_core::RangeOpen;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use tracing::warn;

use crate::{
    recon::{Range, ReconItem, SyncState},
    AssociativeHash, InterestProvider, Key, Metrics, Recon, Store,
};

/// Client to a [`Recon`] [`Server`].
#[derive(Debug, Clone)]
pub struct Client<K, H> {
    sender: Sender<Request<K, H>>,
    metrics: Metrics,
}

impl<K, H> Client<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// Sends an insert request to the server and awaits the response.
    pub async fn insert(&self, key: K, value: Option<Vec<u8>>) -> Result<bool> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::Insert { key, value, ret })
            .await?;
        rx.await?
    }

    /// Sends a len request to the server and awaits the response.
    pub async fn len(&self) -> Result<usize> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::Len { ret }).await?;
        rx.await?
    }

    /// Sends an is_empty request to the server and awaits the response.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Sends a range request to the server and awaits the response.
    pub async fn range(
        &self,
        left_fencepost: K,
        right_fencepost: K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + '_>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::Range {
                left_fencepost,
                right_fencepost,
                offset,
                limit,
                ret,
            })
            .await?;
        rx.await?
    }
    /// Sends a range request to the server and awaits the response.
    pub async fn range_with_values(
        &self,
        left_fencepost: K,
        right_fencepost: K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (K, Vec<u8>)> + Send + '_>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::RangeWithValues {
                left_fencepost,
                right_fencepost,
                offset,
                limit,
                ret,
            })
            .await?;
        rx.await?
    }

    /// Sends a full_range request to the server and awaits the response.
    pub async fn full_range(&self) -> Result<Box<dyn Iterator<Item = K> + Send + '_>> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::FullRange { ret }).await?;
        rx.await?
    }

    /// Sends a full_range request to the server and awaits the response.
    pub async fn value_for_key(&self, key: K) -> Result<Option<Vec<u8>>> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::ValueForKey { key, ret }).await?;
        rx.await?
    }

    /// Report all keys in the range that are missing a value
    pub async fn keys_with_missing_values(&self, range: RangeOpen<K>) -> Result<Vec<K>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::KeysWithMissingValues { range, ret })
            .await?;
        rx.await?
    }
    /// Report the local nodes interests.
    pub async fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::Interests { ret }).await?;
        rx.await?
    }
    /// Compute the intersection of local and remote interests.
    pub async fn process_interests(
        &self,
        interests: Vec<RangeOpen<K>>,
    ) -> Result<Vec<RangeOpen<K>>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::ProcessInterests { interests, ret })
            .await?;
        rx.await?
    }

    /// Compute the hash of a range.
    pub async fn initial_range(&self, interest: RangeOpen<K>) -> Result<Range<K, H>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::InitialRange { interest, ret })
            .await?;
        rx.await?
    }
    /// Compute the synchornization state from a remote range.
    pub async fn process_range(&self, range: Range<K, H>) -> Result<(SyncState<K, H>, Vec<K>)> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::ProcessRange { range, ret })
            .await?;
        rx.await?
    }
    /// Expose metrics
    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }
}

enum Request<K, H> {
    Insert {
        key: K,
        value: Option<Vec<u8>>,
        ret: oneshot::Sender<Result<bool>>,
    },
    Len {
        ret: oneshot::Sender<Result<usize>>,
    },
    Range {
        left_fencepost: K,
        right_fencepost: K,
        offset: usize,
        limit: usize,
        ret: oneshot::Sender<Result<Box<dyn Iterator<Item = K> + Send>>>,
    },
    RangeWithValues {
        left_fencepost: K,
        right_fencepost: K,
        offset: usize,
        limit: usize,
        ret: oneshot::Sender<RangeWithValuesResult<K>>,
    },
    FullRange {
        ret: oneshot::Sender<Result<Box<dyn Iterator<Item = K> + Send>>>,
    },
    ValueForKey {
        key: K,
        ret: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    KeysWithMissingValues {
        range: RangeOpen<K>,
        ret: oneshot::Sender<Result<Vec<K>>>,
    },
    Interests {
        ret: oneshot::Sender<Result<Vec<RangeOpen<K>>>>,
    },
    InitialRange {
        interest: RangeOpen<K>,
        ret: oneshot::Sender<Result<Range<K, H>>>,
    },
    ProcessInterests {
        interests: Vec<RangeOpen<K>>,
        ret: oneshot::Sender<Result<Vec<RangeOpen<K>>>>,
    },
    ProcessRange {
        range: Range<K, H>,
        ret: oneshot::Sender<ProcessRangeResult<K, H>>,
    },
}

type RangeWithValuesResult<K> = Result<Box<dyn Iterator<Item = (K, Vec<u8>)> + Send>>;
type ProcessRangeResult<K, H> = Result<(SyncState<K, H>, Vec<K>)>;

/// Server that processed received Recon messages in a single task.
#[derive(Debug)]
pub struct Server<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync,
    I: InterestProvider<Key = K>,
{
    requests_sender: Sender<Request<K, H>>,
    requests: Receiver<Request<K, H>>,
    recon: Recon<K, H, S, I>,
}

impl<K, H, S, I> Server<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync + 'static,
    I: InterestProvider<Key = K> + 'static,
{
    /// Construct a [`Server`] from a [`Recon`] instance.
    pub fn new(recon: Recon<K, H, S, I>) -> Self {
        let (tx, rx) = channel(1024);
        Self {
            requests_sender: tx,
            requests: rx,
            recon,
        }
    }
    /// Construct a [`Client`] to this server.
    ///
    /// Clients can be safely cloned to create more clients as needed.
    pub fn client(&mut self) -> Client<K, H> {
        Client {
            sender: self.requests_sender.clone(),
            metrics: self.recon.metrics.clone(),
        }
    }

    /// Run the server loop, does not exit until all clients have been dropped.
    pub async fn run(mut self) {
        // Drop the requests_sender first so we only wait for other clients.
        drop(self.requests_sender);
        // Using a single loop ensures Recon methods are never called concurrently.
        // This keeps their implementation simple and as they are modifying local state this is
        // likely the most efficient means to process Recon messages.
        loop {
            let request = self.requests.recv().await;
            if let Some(request) = request {
                match request {
                    Request::Insert { key, value, ret } => {
                        let val = self
                            .recon
                            .insert(&ReconItem::new(&key, value.as_deref()))
                            .await;
                        send(ret, val);
                    }
                    Request::Len { ret } => {
                        send(ret, self.recon.len().await);
                    }
                    Request::Range {
                        left_fencepost,
                        right_fencepost,
                        offset,
                        limit,
                        ret,
                    } => {
                        let keys = self
                            .recon
                            .range(&left_fencepost, &right_fencepost, offset, limit)
                            .await;
                        send(ret, keys);
                    }
                    Request::RangeWithValues {
                        left_fencepost,
                        right_fencepost,
                        offset,
                        limit,
                        ret,
                    } => {
                        let keys = self
                            .recon
                            .range_with_values(&left_fencepost, &right_fencepost, offset, limit)
                            .await;
                        send(ret, keys);
                    }
                    Request::FullRange { ret } => {
                        let keys = self.recon.full_range().await;
                        send(ret, keys);
                    }
                    Request::ValueForKey { key, ret } => {
                        let value = self.recon.value_for_key(key).await;
                        send(ret, value);
                    }
                    Request::KeysWithMissingValues { range, ret } => {
                        let ok = self.recon.keys_with_missing_values(range).await;
                        send(ret, ok);
                    }
                    Request::Interests { ret } => {
                        let value = self.recon.interests().await;
                        send(ret, value);
                    }
                    Request::InitialRange { interest, ret } => {
                        let value = self.recon.initial_range(interest).await;
                        send(ret, value);
                    }
                    Request::ProcessInterests { interests, ret } => {
                        let value = self.recon.process_interests(&interests).await;
                        send(ret, value);
                    }
                    Request::ProcessRange { range, ret } => {
                        let value = self.recon.process_range(range).await;
                        send(ret, value);
                    }
                };
            } else {
                // We are done, all clients have been dropped.
                break;
            }
        }
    }
}

fn send<T>(sender: oneshot::Sender<T>, data: T) {
    match sender.send(data) {
        Ok(_) => {
            // all good, do nothing
        }
        Err(_) => warn!("failed to send recon response on channel"),
    }
}
