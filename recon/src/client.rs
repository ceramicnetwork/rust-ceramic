use anyhow::Result;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use tracing::warn;

use crate::{AssociativeHash, InterestProvider, Key, Message, Recon, Response, Store};

/// Client to a [`Recon`] [`Server`].
#[derive(Debug, Clone)]
pub struct Client<K, H> {
    sender: Sender<Request<K, H>>,
}

impl<K, H> Client<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// Sends an initial_messages request to the server and awaits the response.
    pub async fn initial_messages(&self) -> Result<Vec<Message<K, H>>> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::InitialMessages { ret }).await?;
        rx.await?
    }
    /// Sends a process_messages request to the server and awaits the response.
    pub async fn process_messages(&self, received: Vec<Message<K, H>>) -> Result<Response<K, H>> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::ProcessMessages { received, ret })
            .await?;
        rx.await?
    }

    /// Sends an insert request to the server and awaits the response.
    pub async fn insert(&self, key: K) -> Result<bool> {
        let (ret, rx) = oneshot::channel();
        self.sender.send(Request::Insert { key, ret }).await?;
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
    pub async fn store_value_for_key(&self, key: K, value: &[u8]) -> Result<()> {
        let (ret, rx) = oneshot::channel();
        self.sender
            .send(Request::StoreValueForKey {
                key,
                value: value.to_vec(),
                ret,
            })
            .await?;
        rx.await?
    }
}

enum Request<K, H> {
    InitialMessages {
        ret: oneshot::Sender<Result<Vec<Message<K, H>>>>,
    },
    ProcessMessages {
        received: Vec<Message<K, H>>,
        ret: oneshot::Sender<Result<Response<K, H>>>,
    },
    Insert {
        key: K,
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
    FullRange {
        ret: oneshot::Sender<Result<Box<dyn Iterator<Item = K> + Send>>>,
    },
    ValueForKey {
        key: K,
        ret: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    StoreValueForKey {
        key: K,
        value: Vec<u8>,
        ret: oneshot::Sender<Result<()>>,
    },
}

/// Server that processed received Recon messages in a single task.
#[derive(Debug)]
pub struct Server<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send,
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
    S: Store<Key = K, Hash = H> + Send + 'static,
    I: InterestProvider<Key = K> + 'static,
{
    /// Consturct a [`Server`] from a [`Recon`] instance.
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
                    Request::InitialMessages { ret } => {
                        send(ret, self.recon.initial_messages().await);
                    }
                    Request::ProcessMessages { received, ret } => {
                        send(ret, self.recon.process_messages(&received).await);
                    }
                    Request::Insert { key, ret } => {
                        send(ret, self.recon.insert(&key).await);
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
                    Request::FullRange { ret } => {
                        let keys = self.recon.full_range().await;
                        send(ret, keys);
                    }
                    Request::ValueForKey { key, ret } => {
                        let value = self.recon.value_for_key(key).await;
                        send(ret, value);
                    }
                    Request::StoreValueForKey { key, value, ret } => {
                        let ok = self.recon.store_value_for_key(key, value).await;
                        send(ret, ok);
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
