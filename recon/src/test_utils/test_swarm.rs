use crate::libp2p::Behaviour;
use crate::test_utils::mock_or_real::MockOrRealStore;
use crate::test_utils::test_behaviour::{
    InjectedEvent, InjectingBehavior, InjectorStats, TestBehaviour, TestNetworkBehaviour,
};
use crate::test_utils::{BTreeStore, MockStoreForEventId, MockStoreForInterest};
use crate::{Client, FullInterests, InterestProvider, Key, Metrics, Sha256a, Store};
use ceramic_core::{EventId, Interest};
use futures::{stream::StreamExt, FutureExt};
use libp2p::swarm::NetworkBehaviour;
use libp2p_swarm_test::SwarmExt;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use tokio::sync::Mutex;

/// How to mock stores in the swarm
pub enum MockingType {
    /// Don't mock anything
    None,
    /// Mock interest store
    Interest,
    /// Mock event store
    Event,
    /// Mock both interest and event stores
    All,
}

impl MockingType {
    /// If type is mocking interest
    pub fn mock_interest(&self) -> bool {
        match self {
            Self::Interest | Self::All => true,
            Self::Event | Self::None => false,
        }
    }

    /// If type is mocking event
    pub fn mock_event(&self) -> bool {
        match self {
            Self::Event | Self::All => true,
            Self::Interest | Self::None => false,
        }
    }

    /// Create a test swarm from this type
    pub fn into_swarm(self) -> TestSwarm<StoreBehavior> {
        let event_store = if self.mock_event() {
            MockOrRealStore::Mock(Arc::new(Mutex::new(MockStoreForEventId::new())))
        } else {
            MockOrRealStore::Real(BTreeStore::default())
        };
        let event_recon = crate::Recon::new(
            event_store.clone(),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );
        let event_client = start_recon(event_recon);

        let interest_store = if self.mock_interest() {
            MockOrRealStore::Mock(Arc::new(Mutex::new(MockStoreForInterest::new())))
        } else {
            MockOrRealStore::Real(BTreeStore::default())
        };
        let interest_recon = crate::Recon::new(
            interest_store.clone(),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        );

        let interest_client = start_recon(interest_recon);

        let behaviour = Behaviour::new(
            interest_client,
            event_client,
            crate::libp2p::Config::default(),
        );
        let behaviour = TestBehaviour {
            inner: behaviour,
            convert: Box::new(|_, ev| Some(ev)),
            api: Box::new(|_, _| ()),
        };

        let (tx_events, rx_events) = tokio::sync::mpsc::channel(100);
        let swarm = libp2p::Swarm::new_ephemeral(|_| TestNetworkBehaviour {
            inner: InjectingBehavior::new(rx_events, behaviour),
        });

        TestSwarm::<StoreBehavior> {
            events: tx_events,
            interest_store,
            event_store,
            swarm,
        }
    }
}

/// Information about a swarm being driven
pub struct DrivenSwarm {
    /// Channel to receive swarm events
    events: tokio::sync::mpsc::Sender<InjectedEvent>,
    /// Channel to stop the swarm
    stop: tokio::sync::oneshot::Sender<()>,
    /// Handle to the swarm task
    complete: tokio::task::JoinHandle<InjectorStats>,
}

impl DrivenSwarm {
    /// Inject an event into the swarm
    pub async fn inject(&self, ev: InjectedEvent) {
        let _ = self.events.send(ev).await;
    }

    /// Stop the swarm
    pub async fn stop(self) -> InjectorStats {
        let _ = self.stop.send(());

        self.complete.await.unwrap()
    }
}

/// Container for swarm testing objects
pub struct TestSwarm<N>
where
    N: NetworkBehaviour,
{
    /// Ability to inject events into swarm
    pub events: tokio::sync::mpsc::Sender<InjectedEvent>,
    /// Interests used by recon client connects
    pub interest_store: MockOrRealStore<Interest, MockStoreForInterest>,
    /// Event ids used by recon client connects
    pub event_store: MockOrRealStore<EventId, MockStoreForEventId>,
    /// Swarm instance
    pub swarm: libp2p::Swarm<TestNetworkBehaviour<N>>,
}

fn start_recon<K, H, S, I>(recon: crate::Recon<K, H, S, I>) -> Client<K, H>
where
    K: Key,
    H: crate::AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync + 'static,
    I: InterestProvider<Key = K> + Send + Sync + 'static,
{
    let mut server = crate::Server::new(recon);
    let client = server.client();
    tokio::spawn(server.run());
    client
}

type StoreBehavior = Behaviour<Client<Interest, Sha256a>, Client<EventId, Sha256a>>;

impl<N> TestSwarm<N>
where
    N: NetworkBehaviour + Send,
{
    /// Create a new swarm with behavior
    pub fn from_behaviour(behaviour: TestBehaviour<N>) -> Self {
        let (tx_events, rx_events) = tokio::sync::mpsc::channel(100);
        let swarm = libp2p::Swarm::new_ephemeral(|_| TestNetworkBehaviour {
            inner: InjectingBehavior::new(rx_events, behaviour),
        });
        Self {
            events: tx_events,
            interest_store: MockOrRealStore::Real(BTreeStore::default()),
            event_store: MockOrRealStore::Real(BTreeStore::default()),
            swarm,
        }
    }

    /// Drive the swarm until stopped
    pub fn drive(mut self) -> DrivenSwarm {
        let tx_events = self.events.clone();
        let (tx_stop, rx_stop) = tokio::sync::oneshot::channel();
        let stop_fut = async move {
            let _ = rx_stop.await;
        };
        let mut stop_fut = stop_fut.boxed().fuse();
        let complete = tokio::spawn(async move {
            loop {
                let opt_ev = futures::select! {
                    _ = stop_fut => break,
                    opt_ev = self.swarm.next() => opt_ev,
                };
                if let Some(ev) = opt_ev {
                    tracing::info!("{:?}", ev);
                } else {
                    break;
                }
            }
            self.swarm.behaviour().stats()
        });
        DrivenSwarm {
            events: tx_events,
            stop: tx_stop,
            complete,
        }
    }
}
