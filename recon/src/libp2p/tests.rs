use std::sync::{atomic::AtomicBool, Arc};

use anyhow::{bail, Result};
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_metrics::init_local_tracing;
use libp2p::{metrics::Registry, PeerId, Swarm};
use libp2p_swarm_test::SwarmExt;
use tracing::info;

use crate::{
    libp2p::{stream_set::StreamSet, PeerEvent, PeerStatus},
    AssociativeHash, BTreeStore, FullInterests, HashCount, InsertResult, InterestProvider, Key,
    Metrics, Recon, ReconItem, Server, Store,
};

fn start_recon<K, H, S, I>(recon: Recon<K, H, S, I>) -> crate::Client<K, H>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync + 'static,
    I: InterestProvider<Key = K> + Send + Sync + 'static,
{
    let mut server = Server::new(recon);
    let client = server.client();
    tokio::spawn(server.run());
    client
}

/// An implementation of a Store that stores keys in an in-memory BTree and throws errors if desired.
#[derive(Clone, Debug)]
pub struct BTreeStoreErrors<K, H> {
    should_error: Arc<AtomicBool>,
    inner: BTreeStore<K, H>,
}

impl<K, H> BTreeStoreErrors<K, H> {
    fn set_error(&self, should_error: bool) {
        self.should_error
            .store(should_error, std::sync::atomic::Ordering::SeqCst);
    }
}

impl<K, H> Default for BTreeStoreErrors<K, H> {
    /// By default no errors are thrown. Use set_error to change this.
    fn default() -> Self {
        Self {
            should_error: Arc::new(AtomicBool::new(false)),
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

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: insert")
        } else {
            self.inner.insert(item).await
        }
    }

    async fn insert_many(&self, items: &[ReconItem<'_, K>]) -> Result<InsertResult> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: insert_many")
        } else {
            self.inner.insert_many(items).await
        }
    }

    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> anyhow::Result<HashCount<Self::Hash>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: hash_range")
        } else {
            self.inner.hash_range(left_fencepost, right_fencepost).await
        }
    }

    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: range")
        } else {
            self.inner
                .range(left_fencepost, right_fencepost, offset, limit)
                .await
        }
    }
    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: range_with_values")
        } else {
            self.inner
                .range_with_values(left_fencepost, right_fencepost, offset, limit)
                .await
        }
    }

    async fn last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: last")
        } else {
            self.inner.last(left_fencepost, right_fencepost).await
        }
    }

    async fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: first_and_last")
        } else {
            self.inner
                .first_and_last(left_fencepost, right_fencepost)
                .await
        }
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: value_for_key")
        } else {
            self.inner.value_for_key(key).await
        }
    }
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        if self.should_error.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Sorry, bad tree returns errors: keys_with_missing_values")
        } else {
            self.inner.keys_with_missing_values(range).await
        }
    }
}

// use a hackro to avoid setting all the generic types we'd need if using functions
macro_rules! setup_test {
    ($alice_store: expr, $alice_interests: expr, $bob_store: expr, $bob_interest: expr,) => {{
        let _ = init_local_tracing();

        let alice = Recon::new(
            $alice_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        let alice_client = start_recon(alice);

        let alice_interests = Recon::new(
            $alice_interests,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let alice_interests_client = start_recon(alice_interests);

        let bob_interest = Recon::new(
            $bob_interest,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        let bob_interest_client = start_recon(bob_interest);

        let bob = Recon::new(
            $bob_store,
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        let bob_client = start_recon(bob);

        let swarm1 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(
                alice_interests_client,
                alice_client,
                crate::libp2p::Config::default(),
            )
        });
        let swarm2 = Swarm::new_ephemeral(|_| {
            crate::libp2p::Behaviour::new(
                bob_interest_client,
                bob_client,
                crate::libp2p::Config::default(),
            )
        });

        (swarm1, swarm2)
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initiator_model_error() {
    let alice_model_store = BTreeStoreErrors::default();
    alice_model_store.set_error(true);
    let (mut swarm1, mut swarm2) = setup_test!(
        alice_model_store.clone(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responder_model_error() {
    let bob_model_store = BTreeStoreErrors::default();
    bob_model_store.set_error(true);
    let (mut swarm1, mut swarm2) = setup_test!(
        BTreeStoreErrors::default(),
        BTreeStoreErrors::default(),
        bob_model_store.clone(),
        BTreeStoreErrors::default(),
    );

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        let ([p1_e1, p1_e2, p1_e3], [p2_e1, p2_e2, p2_e3]): (
            [crate::libp2p::Event; 3],
            [crate::libp2p::Event; 3],
        ) = libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        for ev in &[p1_e1, p1_e2, p1_e3, p2_e1, p2_e2, p2_e3] {
            info!("{:?}", ev);
        }
        let ([], [failed_peer]): ([crate::libp2p::Event; 0], [crate::libp2p::Event; 1]) =
            libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

        info!("{:?}", failed_peer);
        assert_eq!(
            failed_peer,
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
