use anyhow::{bail, Result};
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_metrics::init_local_tracing;
use libp2p::{metrics::Registry, Swarm};
use libp2p_swarm_test::SwarmExt;
use rand::Rng as _;
use tracing::info;

use crate::{
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

/// An implementation of a Store that stores keys in an in-memory BTree
#[derive(Clone, Debug)]
pub struct BadBTreeStore<K, H> {
    inner: BTreeStore<K, H>,
}

impl<K, H> Default for BadBTreeStore<K, H> {
    fn default() -> Self {
        Self {
            inner: BTreeStore::default(),
        }
    }
}

fn random_error() -> Result<()> {
    if rand::thread_rng().gen_bool(0.99) {
        bail!("Sorry, you get a random error")
    }
    Ok(())
}

#[async_trait]
impl<K, H> crate::recon::Store for BadBTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        self.inner.insert(item).await
    }

    async fn insert_many(&self, items: &[ReconItem<'_, K>]) -> Result<InsertResult> {
        self.inner.insert_many(items).await
    }

    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> anyhow::Result<HashCount<Self::Hash>> {
        random_error()?;

        self.inner.hash_range(left_fencepost, right_fencepost).await
    }

    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        random_error()?;

        self.inner
            .range(left_fencepost, right_fencepost, offset, limit)
            .await
    }
    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.inner
            .range_with_values(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        self.inner.last(left_fencepost, right_fencepost).await
    }

    async fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        self.inner
            .first_and_last(left_fencepost, right_fencepost)
            .await
    }

    /// value_for_key returns an Error is retrieving failed and None if the key is not stored.
    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.inner.value_for_key(key).await
    }
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        self.inner.keys_with_missing_values(range).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_errors() {
    let _ = init_local_tracing();
    // use the libp2p test swarm to drive the test
    // trivial recon sync with a single peer that crashes
    // create a mock store impl that crashes based on input

    let alice = Recon::new(
        BTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let alice_client = start_recon(alice);

    let alice_interests = Recon::new(
        BTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );

    let alice_interests_client = start_recon(alice_interests);

    let bob_interest = Recon::new(
        BadBTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let bob_interest_client = start_recon(bob_interest);

    let bob = Recon::new(
        BadBTreeStore::default(),
        FullInterests::default(),
        Metrics::register(&mut Registry::default()),
    );
    let bob_client = start_recon(bob);

    let mut swarm1 = Swarm::new_ephemeral(|_| {
        crate::libp2p::Behaviour::new(
            alice_interests_client,
            alice_client,
            crate::libp2p::Config::default(),
        )
    });
    let mut swarm2 = Swarm::new_ephemeral(|_| {
        crate::libp2p::Behaviour::new(
            bob_interest_client,
            bob_client,
            crate::libp2p::Config::default(),
        )
    });

    let fut = async move {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;

        for cnt in 0..3 {
            let ([e1], [e2]): ([crate::libp2p::Event; 1], [crate::libp2p::Event; 1]) =
                libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await;

            // TODO: with the 99% change of a "random" error, I'm not seeing the 4th message and things just end

            info!("{cnt}: {:?} == {:?}", e1, e2);
            /*
              2024-04-17T22:05:56.462337Z  INFO recon::libp2p::tests: 0: PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWHCT2YaKxkRA3627i6LUJTdFiBeG8hz9pb9kQfziYFD8p"), status: Started { stream_set: Interest } }) == PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWBsigNTGsXd5r4etCi5endtSoLjyD9LftyXSpdNwUnboe"), status: Started { stream_set: Interest } })
                at recon/src/libp2p/tests.rs:85

              2024-04-17T22:05:56.463717Z  INFO recon::libp2p::tests: 1: PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWHCT2YaKxkRA3627i6LUJTdFiBeG8hz9pb9kQfziYFD8p"), status: Synchronized { stream_set: Interest } }) == PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWBsigNTGsXd5r4etCi5endtSoLjyD9LftyXSpdNwUnboe"), status: Synchronized { stream_set: Interest } })
                at recon/src/libp2p/tests.rs:85

              2024-04-17T22:05:56.463851Z  INFO recon::libp2p::tests: 2: PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWHCT2YaKxkRA3627i6LUJTdFiBeG8hz9pb9kQfziYFD8p"), status: Started { stream_set: Model } }) == PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWBsigNTGsXd5r4etCi5endtSoLjyD9LftyXSpdNwUnboe"), status: Started { stream_set: Model } })
                at recon/src/libp2p/tests.rs:85

              2024-04-17T22:05:56.464592Z  INFO recon::libp2p::tests: 3: PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWHCT2YaKxkRA3627i6LUJTdFiBeG8hz9pb9kQfziYFD8p"), status: Synchronized { stream_set: Model } }) == PeerEvent(PeerEvent { remote_peer_id: PeerId("12D3KooWBsigNTGsXd5r4etCi5endtSoLjyD9LftyXSpdNwUnboe"), status: Synchronized { stream_set: Model } })
                at recon/src/libp2p/tests.rs:85
            */
        }
    };

    fut.await;
}
