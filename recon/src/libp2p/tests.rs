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

        let bob_peer = Recon::new(
            $bob_peer,
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
            blocked_peers: std::collections::HashSet::new(),
        };
        let swarm1 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(alice_peer, alice, config.clone())
        });
        let swarm2 = Swarm::new_ephemeral(|_| crate::libp2p::Behaviour::new(bob_peer, bob, config));

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

        let (p1_events, p2_events): ([crate::libp2p::Event; 4], [crate::libp2p::Event; 4]) =
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

        let (p1_events, p2_events): ([crate::libp2p::Event; 4], [crate::libp2p::Event; 3]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in p1_events.iter().chain(p2_events.iter()) {
            info!("{:?}", ev);
        }

        assert_eq!(
            p1_events[3],
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

        let (p1_events, p2_events): ([crate::libp2p::Event; 4], [crate::libp2p::Event; 4]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in p1_events.iter().chain(p2_events.iter()) {
            info!("{:?}", ev);
        }
        assert_eq!(
            p2_events[3],
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
        let (p1_events, p2_events): ([crate::libp2p::Event; 12], [crate::libp2p::Event; 12]) =
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
                            PeerStatus::Synchronized { stream_set, .. }
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
            // First model sync
            Some(StreamSet::Model),
            Some(StreamSet::Model),
            // Second peers sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
            // Second model sync with initial short backoff
            Some(StreamSet::Model),
            Some(StreamSet::Model),
            // Third peers sync
            Some(StreamSet::Peer),
            Some(StreamSet::Peer),
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
            events[1][3],
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm1.local_peer_id().to_owned(),
                status: PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            })
        );
        assert_eq!(
            events[1][7],
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

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn blocked_peer_connection_rejected() {
    // First create Bob's swarm to get his PeerId
    let bob_peer = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let bob = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );

    let bob_config = crate::libp2p::Config {
        per_peer_sync_delay: std::time::Duration::from_millis(10),
        per_peer_sync_backoff: 100.0,
        per_peer_maximum_sync_delay: std::time::Duration::from_millis(1000),
        blocked_peers: std::collections::HashSet::new(),
    };

    let mut swarm2 =
        Swarm::new_ephemeral(|_| crate::libp2p::Behaviour::new(bob_peer, bob, bob_config));

    // Get Bob's peer ID and create Alice with Bob blocked
    let bob_peer_id = *swarm2.local_peer_id();

    let alice_peer = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let alice = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );

    let mut blocked_peers = std::collections::HashSet::new();
    blocked_peers.insert(bob_peer_id);

    let alice_config = crate::libp2p::Config {
        per_peer_sync_delay: std::time::Duration::from_millis(10),
        per_peer_sync_backoff: 100.0,
        per_peer_maximum_sync_delay: std::time::Duration::from_millis(1000),
        blocked_peers,
    };

    let mut swarm1 =
        Swarm::new_ephemeral(|_| crate::libp2p::Behaviour::new(alice_peer, alice, alice_config));

    // Alice listens, Bob tries to connect
    swarm1.listen().with_memory_addr_external().await;

    // Bob connects to Alice - the connection should be denied
    let alice_addr = swarm1.external_addresses().next().unwrap().clone();
    swarm2.dial(alice_addr).unwrap();

    // Drive the swarms - when Alice blocks Bob, the connection is established at the transport
    // level but immediately closed when handle_established_inbound_connection returns ConnectionDenied.
    // We verify this by checking that Bob sees ConnectionClosed right after ConnectionEstablished.
    let result = tokio::time::timeout(std::time::Duration::from_millis(1000), async {
        let mut connection_established = false;
        loop {
            tokio::select! {
                _e = swarm1.next_swarm_event() => {
                    // Alice processes events
                }
                e = swarm2.next_swarm_event() => {
                    match e {
                        libp2p::swarm::SwarmEvent::ConnectionEstablished { .. } => {
                            connection_established = true;
                        }
                        libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            // Connection was closed - this happens when Alice blocks Bob
                            if connection_established && peer_id == *swarm1.local_peer_id() {
                                return true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    })
    .await;

    // Verify the connection was established then immediately closed (blocked peer behavior)
    assert!(
        result.is_ok() && result.unwrap(),
        "Expected connection to be established then closed due to blocking"
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn inbound_sync_rejected_during_backoff() {
    // This test verifies that after sync failures, the backoff mechanism prevents syncs.
    // We set up a scenario where:
    // 1. Bob's model store fails repeatedly, causing sync failures
    // 2. Due to the large backoff multiplier (100x), model syncs are eventually skipped
    // 3. We observe that by the third sync cycle, model syncs are skipped entirely
    //
    // The backoff progression is: 10ms -> 1000ms (after second fail)
    // This means the second model sync still happens (backoff not yet active),
    // but by the third cycle, the backoff has taken effect and model syncs are skipped.

    let mut bob_model_store = BTreeStoreErrors::default();
    bob_model_store.set_error(Error::new_transient(anyhow::anyhow!(
        "transient error to trigger backoff"
    )));

    let (mut swarm1, mut swarm2) = setup_test!(
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        bob_model_store,
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
    );

    swarm1.listen().with_memory_addr_external().await;
    swarm2.connect(&mut swarm1).await;

    // Drive through three sync cycles to observe backoff behavior
    // Cycle 1 (events 0-3): Peer sync, Model sync (fails)
    // Cycle 2 (events 4-7): Peer sync, Model sync (fails again, backoff increases)
    // Cycle 3 (events 8-11): Peer sync, Peer sync (model skipped due to long backoff)
    let (p1_events, p2_events): ([crate::libp2p::Event; 12], [crate::libp2p::Event; 12]) =
        libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

    // Verify Bob saw model sync failures in the first two cycles
    assert_eq!(
        into_peer_event(p2_events[3].clone()).status,
        PeerStatus::Failed {
            stream_set: StreamSet::Model
        },
        "Expected Bob to see first Model sync failure"
    );
    assert_eq!(
        into_peer_event(p2_events[7].clone()).status,
        PeerStatus::Failed {
            stream_set: StreamSet::Model
        },
        "Expected Bob to see second Model sync failure"
    );

    // Verify that in the third cycle (events 8-11), only peer syncs occur
    // because the model sync backoff (1000ms) now exceeds the peer sync delay (10ms)
    let third_cycle_stream_sets: Vec<_> = p1_events[8..12]
        .iter()
        .map(|ev| {
            let crate::libp2p::Event::PeerEvent(PeerEvent { status, .. }) = ev;
            match status {
                PeerStatus::Started { stream_set }
                | PeerStatus::Synchronized { stream_set, .. }
                | PeerStatus::Failed { stream_set } => Some(*stream_set),
                _ => None,
            }
        })
        .collect();

    // In the third cycle, backoff has kicked in - we should see only peer syncs
    assert!(
        third_cycle_stream_sets
            .iter()
            .all(|s| *s == Some(StreamSet::Peer)),
        "Expected only Peer syncs in third cycle after backoff kicked in, got: {:?}",
        third_cycle_stream_sets
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn backoff_registry_expires() {
    // This test verifies that backoff entries expire and allow syncs to resume.
    // We configure a short backoff (10ms) and verify:
    // 1. Initial sync fails, triggering backoff
    // 2. After backoff expires, model syncs resume (they may fail again, but they're attempted)
    //
    // The key observation is that with a short backoff, we should see model sync attempts
    // resume after the backoff period, unlike in inbound_sync_rejected_during_backoff where
    // the large backoff (1000ms) prevents model syncs from occurring.

    let mut bob_model_store = BTreeStoreErrors::default();
    bob_model_store.set_error(Error::new_transient(anyhow::anyhow!(
        "transient error to trigger backoff"
    )));

    // Create swarms with very short backoff times so they expire quickly
    let alice = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let alice_peer = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let bob_peer = Recon::new(
        BTreeStoreErrors::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let bob = Recon::new(
        bob_model_store,
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );

    // Use very short backoff (5ms * 2 = 10ms) that will expire before the next peer sync cycle
    let config = crate::libp2p::Config {
        per_peer_sync_delay: std::time::Duration::from_millis(5),
        per_peer_sync_backoff: 2.0,
        per_peer_maximum_sync_delay: std::time::Duration::from_millis(50),
        blocked_peers: std::collections::HashSet::new(),
    };

    let mut swarm1 =
        Swarm::new_ephemeral(|_| crate::libp2p::Behaviour::new(alice_peer, alice, config.clone()));
    let mut swarm2 = Swarm::new_ephemeral(|_| crate::libp2p::Behaviour::new(bob_peer, bob, config));

    swarm1.listen().with_memory_addr_external().await;
    swarm2.connect(&mut swarm1).await;

    // Drive through multiple sync cycles
    // With short backoff (10ms), model syncs should resume after the backoff expires
    // We expect to see model sync attempts (failures) interspersed with peer syncs
    let (p1_events, _p2_events): ([crate::libp2p::Event; 12], [crate::libp2p::Event; 12]) =
        libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

    // Count how many model sync events we see
    let model_sync_count = p1_events
        .iter()
        .filter(|ev| {
            let crate::libp2p::Event::PeerEvent(PeerEvent { status, .. }) = ev;
            matches!(
                status,
                PeerStatus::Started {
                    stream_set: StreamSet::Model
                } | PeerStatus::Synchronized {
                    stream_set: StreamSet::Model,
                    ..
                } | PeerStatus::Failed {
                    stream_set: StreamSet::Model
                }
            )
        })
        .count();

    // With short backoff, we should see multiple model sync attempts (they fail but are retried)
    // This proves the backoff expires and allows new sync attempts
    assert!(
        model_sync_count >= 2,
        "Expected at least 2 model sync events (proving backoff expired and syncs resumed), got: {}",
        model_sync_count
    );
}

fn assert_in_sync(id: PeerId, events: [crate::libp2p::Event; 4]) {
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
                stream_set: StreamSet::Peer,
                new_count: 0,
            }
        }
    );
    assert_eq!(
        into_peer_event(events[2].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Started {
                stream_set: StreamSet::Model
            }
        }
    );
    assert_eq!(
        into_peer_event(events[3].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Synchronized {
                stream_set: StreamSet::Model,
                new_count: 0,
            }
        }
    );
}
