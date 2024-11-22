use std::{ops::Range, sync::Arc};

use crate::{
    libp2p::{stream_set::StreamSet, PeerEvent, PeerStatus},
    AssociativeHash, BTreeStore, Error, FullInterests, HashCount, InsertResult, Key, Metrics,
    Recon, ReconItem, Result as ReconResult,
};

use async_trait::async_trait;
use ceramic_core::NodeId;
use libp2p::{metrics::Registry, PeerId, Swarm};
use libp2p_swarm_test::SwarmExt;
use test_log::test;
use tracing::info;

/// An implementation of a Store that stores keys in an in-memory BTree and throws errors if desired.
#[derive(Debug, Clone)]
pub struct BTreeStoreErrors<K, H> {
    error: Option<Arc<Error>>,
    inner: BTreeStore<K, H>,
}

impl<K, H> BTreeStoreErrors<K, H> {
    fn set_error(&mut self, error: Error) {
        self.error = Some(Arc::new(error));
    }

    fn as_error(&self) -> Result<(), Error> {
        if let Some(err) = &self.error {
            match err.as_ref() {
                Error::Application { error } => Err(Error::Application {
                    error: anyhow::anyhow!(error.to_string()),
                }),
                Error::Fatal { error } => Err(Error::Fatal {
                    error: anyhow::anyhow!(error.to_string()),
                }),
                Error::Transient { error } => Err(Error::Transient {
                    error: anyhow::anyhow!(error.to_string()),
                }),
            }
        } else {
            Ok(())
        }
    }
}

impl<K, H> Default for BTreeStoreErrors<K, H> {
    /// By default no errors are thrown. Use set_error to change this.
    fn default() -> Self {
        Self {
            error: None,
            inner: BTreeStore::default(),
        }
    }
}

#[async_trait]
impl<K, H> crate::recon::Store for BTreeStoreErrors<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    async fn insert_many(
        &self,
        items: &[ReconItem<K>],
        informant: NodeId,
    ) -> ReconResult<InsertResult<Self::Key>> {
        self.as_error()?;

        self.inner.insert_many(items, informant).await
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        self.as_error()?;

        self.inner.hash_range(range).await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.as_error()?;

        self.inner.range(range).await
    }
    async fn first(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        self.as_error()?;

        self.inner.first(range).await
    }
    async fn middle(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        self.as_error()?;

        self.inner.middle(range).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        self.as_error()?;

        self.inner.value_for_key(key).await
    }
}

// use a hackro to avoid setting all the generic types we'd need if using functions
macro_rules! setup_test {
    ($alice_store: expr, $alice_peer: expr, $alice_interest: expr, $bob_store: expr, $bob_peer: expr, $bob_interest: expr,) => {{
        let alice = Recon::new(
            $alice_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let alice_peer = Recon::new(
            $alice_peer,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let alice_interest = Recon::new(
            $alice_interest,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let bob_peer = Recon::new(
            $bob_peer,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let bob_interest = Recon::new(
            $bob_interest,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let bob = Recon::new(
            $bob_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        // Use shorter timings with large backoff for testing.
        let config = crate::libp2p::Config {
            per_peer_sync_delay: std::time::Duration::from_millis(10),
            per_peer_sync_backoff: 100.0,
            per_peer_maximum_sync_delay: std::time::Duration::from_millis(1000),
        };
        let swarm1 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(alice_peer, alice_interest, alice, config.clone())
        });
        let swarm2 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(bob_peer, bob_interest, bob, config)
        });

        (swarm1, swarm2)
    }};
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn in_sync_no_overlap() {
    let (mut swarm1, mut swarm2) = setup_test!(
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        let p1 = swarm1.local_peer_id().to_owned();
        let p2 = swarm2.local_peer_id().to_owned();

        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let (p1_events, p2_events): ([crate::libp2p::Event; 6], [crate::libp2p::Event; 6]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        assert_in_sync(p2, p1_events);
        assert_in_sync(p1, p2_events);
    };

    fut.await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn initiator_model_error() {
    let mut alice_model_store = BTreeStoreErrors::default();
    alice_model_store.set_error(Error::new_transient(anyhow::anyhow!(
        "transient error should be handled"
    )));
    let (mut swarm1, mut swarm2) = setup_test!(
        alice_model_store,
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let (p1_events, p2_events): ([crate::libp2p::Event; 6], [crate::libp2p::Event; 5]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in p1_events.iter().chain(p2_events.iter()) {
            info!("{:?}", ev);
        }

        assert_eq!(
            p1_events[5],
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm2.local_peer_id().to_owned(),
                status: PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            })
        );
    };

    fut.await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn responder_model_error() {
    let mut bob_model_store = BTreeStoreErrors::default();
    bob_model_store.set_error(Error::new_transient(anyhow::anyhow!(
        "transient error should be handled"
    )));
    let (mut swarm1, mut swarm2) = setup_test!(
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        bob_model_store,
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let (p1_events, p2_events): ([crate::libp2p::Event; 6], [crate::libp2p::Event; 6]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in p1_events.iter().chain(p2_events.iter()) {
            info!("{:?}", ev);
        }
        assert_eq!(
            p2_events[5],
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm1.local_peer_id().to_owned(),
                status: PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            })
        );
    };

    fut.await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn model_error_backoff() {
    let mut bob_model_store = BTreeStoreErrors::default();
    bob_model_store.set_error(Error::new_transient(anyhow::anyhow!(
        "transient error should be handled"
    )));
    let (mut swarm1, mut swarm2) = setup_test!(
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        bob_model_store,
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        // Expect interests to sync twice in a row since models fail to sync
        let (p1_events, p2_events): ([crate::libp2p::Event; 18], [crate::libp2p::Event; 18]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        let events = [p1_events, p2_events];

        for ev in events.iter().flatten() {
            info!("{:?}", ev);
        }
        let stream_sets: Vec<_> = events
            .iter()
            .map(|peer_events| {
                peer_events
                    .iter()
                    .map(|ev| {
                        let crate::libp2p::Event::PeerEvent(PeerEvent { status, .. }) = ev;
                        match status {
                            PeerStatus::Waiting | PeerStatus::Stopped => None,
                            PeerStatus::Synchronized { stream_set }
                            | PeerStatus::Started { stream_set }
                            | PeerStatus::Failed { stream_set } => Some(*stream_set),
                        }
                    })
                    .collect::<Vec<Option<crate::libp2p::StreamSet>>>()
            })
            .collect();
        let expected_stream_set_order = vec![
            // First peers sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
            // First interests sync
            Some(StreamSet::Interest),
            Some(StreamSet::Interest),
            // First model sync
            Some(StreamSet::Model),
            Some(StreamSet::Model),
            // Second peers sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
            // Second interests sync
            Some(StreamSet::Interest),
            Some(StreamSet::Interest),
            // Second model sync with initial short backoff
            Some(StreamSet::Model),
            Some(StreamSet::Model),
            // Third peers sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
            // Third interests sync
            Some(StreamSet::Interest),
            Some(StreamSet::Interest),
            // Third model sync is skipped because the backoff pushed it past the peer sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
        ];
        assert_eq!(
            stream_sets,
            vec![expected_stream_set_order.clone(), expected_stream_set_order]
        );
        // Assert we saw the errors
        assert_eq!(
            events[1][5],
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm1.local_peer_id().to_owned(),
                status: PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            })
        );
        assert_eq!(
            events[1][11],
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm1.local_peer_id().to_owned(),
                status: PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            })
        );
    };

    fut.await;
}

fn into_peer_event(ev: crate::libp2p::Event) -> PeerEvent {
    match ev {
        super::Event::PeerEvent(p) => p,
    }
}

fn assert_in_sync(id: PeerId, events: [crate::libp2p::Event; 6]) {
    assert_eq!(
        into_peer_event(events[0].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Started {
                stream_set: StreamSet::Peer
            }
        }
    );
    assert_eq!(
        into_peer_event(events[1].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Synchronized {
                stream_set: StreamSet::Peer
            }
        }
    );
    assert_eq!(
        into_peer_event(events[2].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Started {
                stream_set: StreamSet::Interest
            }
        }
    );
    assert_eq!(
        into_peer_event(events[3].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Synchronized {
                stream_set: StreamSet::Interest
            }
        }
    );
    assert_eq!(
        into_peer_event(events[4].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Started {
                stream_set: StreamSet::Model
            }
        }
    );
    assert_eq!(
        into_peer_event(events[5].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Synchronized {
                stream_set: StreamSet::Model
            }
        }
    );
}
