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
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.as_error()?;

        self.inner.range(range, offset, limit).await
    }
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.as_error()?;

        self.inner.range_with_values(range, offset, limit).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        self.as_error()?;

        self.inner.value_for_key(key).await
    }
}

// use a hackro to avoid setting all the generic types we'd need if using functions
macro_rules! setup_test {
    ($alice_store: expr, $alice_interest: expr, $bob_store: expr, $bob_interest: expr,) => {{
        let alice = Recon::new(
            $alice_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let alice_interest = Recon::new(
            $alice_interest,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let bob_interests = Recon::new(
            $bob_interest,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let bob = Recon::new(
            $bob_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let swarm1 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(alice_interest, alice, crate::libp2p::Config::default())
        });
        let swarm2 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(bob_interests, bob, crate::libp2p::Config::default())
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
    );

    let fut = async move {
        let p1 = swarm1.local_peer_id().to_owned();
        let p2 = swarm2.local_peer_id().to_owned();

        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let ([p1_e1, p1_e2, p1_e3, p1_e4], [p2_e1, p2_e2, p2_e3, p2_e4]): (
            [crate::libp2p::Event; 4],
            [crate::libp2p::Event; 4],
        ) = libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        assert_in_sync(p2, [p1_e1, p1_e2, p1_e3, p1_e4]);
        assert_in_sync(p1, [p2_e1, p2_e2, p2_e3, p2_e4]);
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
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let ([p1_e1, p1_e2, p1_e3, failed_peer], [p2_e1, p2_e2, p2_e3]): (
            [crate::libp2p::Event; 4],
            [crate::libp2p::Event; 3],
        ) = libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in &[p1_e1, p1_e2, p1_e3, p2_e1, p2_e2, p2_e3] {
            info!("{:?}", ev);
        }

        info!("{:?}", failed_peer);
        assert_eq!(
            failed_peer,
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm2.local_peer_id().to_owned(),
                status: PeerStatus::Failed
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
        bob_model_store,
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let ([p1_e1, p1_e2, p1_e3, p1_e4], [p2_e1, p2_e2, p2_e3, p2_e4]): (
            [crate::libp2p::Event; 4],
            [crate::libp2p::Event; 4],
        ) = libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in [
            &p1_e1, &p1_e2, &p1_e3, &p1_e4, &p2_e1, &p2_e2, &p2_e3, &p2_e4,
        ] {
            info!("{:?}", ev);
        }
        assert_eq!(
            p2_e4,
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: swarm1.local_peer_id().to_owned(),
                status: PeerStatus::Failed
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

fn assert_in_sync(id: PeerId, events: [crate::libp2p::Event; 4]) {
    assert_eq!(
        into_peer_event(events[0].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Started {
                stream_set: StreamSet::Interest
            }
        }
    );
    assert_eq!(
        into_peer_event(events[1].clone()),
        PeerEvent {
            remote_peer_id: id,
            status: PeerStatus::Synchronized {
                stream_set: StreamSet::Interest
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
                stream_set: StreamSet::Model
            }
        }
    );
}
