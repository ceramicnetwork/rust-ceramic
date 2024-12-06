use std::{collections::BTreeSet, ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use ceramic_core::{NodeId, NodeKey, PeerEntry, PeerKey, RangeOpen};
use libp2p::Multiaddr;
use recon::InterestProvider;
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tracing::{debug, warn};

/// [`InterestProvider`] that is interested in [`PeerKey`]s that have not expired.
#[derive(Debug, Clone)]
pub struct PeerKeyInterests;

#[async_trait]
impl InterestProvider for PeerKeyInterests {
    type Key = PeerKey;

    async fn interests(&self) -> recon::Result<Vec<RangeOpen<Self::Key>>> {
        let now = chrono::Utc::now().timestamp() as u64;
        Ok(vec![(
            PeerKey::builder().with_expiration(now).build_fencepost(),
            PeerKey::builder().with_max_expiration().build_fencepost(),
        )
            .into()])
    }
}

#[async_trait]
pub trait PeerService: Send + Sync {
    async fn insert(&self, peer: &PeerKey) -> anyhow::Result<()>;
    async fn delete_range(&self, range: Range<&PeerKey>) -> anyhow::Result<()>;
    async fn all_peers(&self) -> anyhow::Result<Vec<PeerKey>>;
}

#[async_trait]
impl<S: PeerService> PeerService for Arc<S> {
    async fn insert(&self, peer: &PeerKey) -> anyhow::Result<()> {
        self.as_ref().insert(peer).await
    }
    async fn delete_range(&self, range: Range<&PeerKey>) -> anyhow::Result<()> {
        self.as_ref().delete_range(range).await
    }
    async fn all_peers(&self) -> anyhow::Result<Vec<PeerKey>> {
        self.as_ref().all_peers().await
    }
}

#[derive(Debug)]
pub enum Message {
    /// Inform the peers loop about new local addresses.
    NewLocalAddresses(Vec<Multiaddr>),
    /// Inform the peers loop about a local address that is no longer valid.
    RemoveLocalAddress(Multiaddr),
    /// Report a list of all remote peers.
    #[allow(dead_code)] // This will be used as part of issue #606
    AllRemotePeers(oneshot::Sender<anyhow::Result<Vec<PeerEntry>>>),
}

/// Run a loop handling messages and publishing the local node into the Peer recon ring.
/// The local node its expiration time will be set `expiration` duration in the future
/// and published at twice the frequency that it expires.
pub async fn run(
    expiration: Duration,
    node_key: NodeKey,
    svc: impl PeerService,
    mut messages: mpsc::Receiver<Message>,
) {
    let mut addresses = BTreeSet::new();
    let mut interval = tokio::time::interval(expiration / 2);
    loop {
        select! {
            _ = interval.tick() => {
                do_tick(expiration, &node_key, addresses.iter().cloned().collect(), &svc).await
            }
            Some(m) = messages.recv() => {
                if handle_message(node_key.id(), m, &mut addresses,&svc).await{
                    do_tick(expiration, &node_key, addresses.iter().cloned().collect(), &svc).await
                }
            }
        }
    }
}
async fn do_tick(
    expiration: Duration,
    node_key: &NodeKey,
    addressess: Vec<Multiaddr>,
    svc: &impl PeerService,
) {
    // Publish a new peer key with a new expiration.
    // Otherwise other peers will forget about the local node.
    let now = chrono::Utc::now().timestamp() as u64;
    let expiration = now + expiration.as_secs();
    let key = PeerKey::builder()
        .with_expiration(expiration)
        .with_id(node_key)
        .with_addresses(addressess)
        .build();
    if let Err(err) = svc.insert(&key).await {
        warn!(%err, "error encountered publishing local node");
    }
    // Delete all expired peers, otherwise the db would grow indefinitely
    if let Err(err) = svc
        .delete_range(
            &PeerKey::builder().with_min_expiration().build_fencepost()
                ..&PeerKey::builder().with_expiration(now).build_fencepost(),
        )
        .await
    {
        warn!(%err, "error encountered deleting expired peer keys");
    }
}
async fn handle_message(
    node_id: NodeId,
    message: Message,
    addressess: &mut BTreeSet<Multiaddr>,
    svc: &impl PeerService,
) -> bool {
    debug!(%node_id, ?message, "handle_message");
    match message {
        Message::NewLocalAddresses(address) => {
            addressess.extend(address.into_iter());
            true
        }
        Message::RemoveLocalAddress(address) => {
            addressess.remove(&address);
            true
        }
        Message::AllRemotePeers(tx) => {
            let r = match svc.all_peers().await {
                Ok(all_peers) => Ok(all_peers
                    .into_iter()
                    .filter_map(|peer_key| {
                        //Skip any peer keys that do not have a valid signature
                        peer_key.to_entry().ok().and_then(|peer_entry| {
                            // Skip the local node, we only want remote peers
                            if peer_entry.id() != node_id {
                                Some(peer_entry)
                            } else {
                                None
                            }
                        })
                    })
                    .collect()),
                Err(err) => Err(err),
            };
            if tx.send(r).is_err() {
                warn!("failed to send all peers response");
            }
            false
        }
    }
}
