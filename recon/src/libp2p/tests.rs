use libp2p::PeerId;
use tracing::info;

use crate::{
    test_utils::{MockReconForEventId, MockReconForInterest, MockingType, PEER_ID},
    Error,
};
use futures::future::poll_immediate;
use libp2p::core::{ConnectedPoint, Endpoint};
use libp2p::swarm::ConnectionId;
use libp2p::swarm::{behaviour::ConnectionEstablished, NetworkBehaviour};
use libp2p::Multiaddr;
use libp2p_swarm_test::SwarmExt;
use std::future::poll_fn;
use std::pin::pin;

use crate::libp2p::{stream_set::StreamSet, PeerEvent, PeerStatus};
use crate::libp2p::{Behaviour, Config};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_sync_no_overlap() {
    let mut a = MockingType::None.into_swarm();
    let mut b = MockingType::None.into_swarm();

    let fut = async move {
        let p1 = a.swarm.local_peer_id().to_owned();
        let p2 = b.swarm.local_peer_id().to_owned();

        a.swarm.listen().with_memory_addr_external().await;
        b.swarm.connect(&mut a.swarm).await;

        let ([p1_e1, p1_e2, p1_e3, p1_e4], [p2_e1, p2_e2, p2_e3, p2_e4]): (
            [crate::libp2p::Event; 4],
            [crate::libp2p::Event; 4],
        ) = libp2p_swarm_test::drive(&mut a.swarm, &mut b.swarm).await;

        assert_in_sync(p2, [p1_e1, p1_e2, p1_e3, p1_e4]);
        assert_in_sync(p1, [p2_e1, p2_e2, p2_e3, p2_e4]);
    };

    fut.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initiator_model_error() {
    let mut a = MockingType::Event.into_swarm();
    let mut b = MockingType::None.into_swarm();

    // fail the model sync
    let _ = {
        a.event_store
            .as_mock()
            .lock()
            .await
            .expect_hash_range()
            .returning(|_, _| Err(Error::new_app(anyhow::anyhow!("error"))))
    };

    let fut = async move {
        a.swarm.listen().with_memory_addr_external().await;
        b.swarm.connect(&mut a.swarm).await;

        let ([p1_e1, p1_e2, p1_e3, failed_peer], [p2_e1, p2_e2, p2_e3]): (
            [crate::libp2p::Event; 4],
            [crate::libp2p::Event; 3],
        ) = libp2p_swarm_test::drive(&mut a.swarm, &mut b.swarm).await;

        for ev in &[p1_e1, p1_e2, p1_e3, p2_e1, p2_e2, p2_e3] {
            info!("{:?}", ev);
        }

        info!("{:?}", failed_peer);
        assert_eq!(
            failed_peer,
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: b.swarm.local_peer_id().to_owned(),
                status: PeerStatus::Failed
            })
        );
    };

    fut.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responder_model_error() {
    let mut a = MockingType::None.into_swarm();
    let mut b = MockingType::Event.into_swarm();

    // fail the model sync
    let _ = {
        b.event_store
            .as_mock()
            .lock()
            .await
            .expect_hash_range()
            .returning(|_, _| Err(Error::new_app(anyhow::anyhow!("error"))))
    };

    let fut = async move {
        a.swarm.listen().with_memory_addr_external().await;
        b.swarm.connect(&mut a.swarm).await;

        let ([p1_e1, p1_e2, p1_e3], [p2_e1, p2_e2, p2_e3]): (
            [crate::libp2p::Event; 3],
            [crate::libp2p::Event; 3],
        ) = libp2p_swarm_test::drive(&mut a.swarm, &mut b.swarm).await;

        for ev in &[p1_e1, p1_e2, p1_e3, p2_e1, p2_e2, p2_e3] {
            info!("{:?}", ev);
        }
        let ([], [failed_peer]): ([crate::libp2p::Event; 0], [crate::libp2p::Event; 1]) =
            libp2p_swarm_test::drive(&mut a.swarm, &mut b.swarm).await;

        info!("{:?}", failed_peer);
        assert_eq!(
            failed_peer,
            crate::libp2p::Event::PeerEvent(PeerEvent {
                remote_peer_id: a.swarm.local_peer_id().to_owned(),
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

#[tokio::test(flavor = "multi_thread")]
async fn poll_not_ready_on_connection_established() {
    let mut behavior = Behaviour::new(
        MockReconForInterest::default(),
        MockReconForEventId::default(),
        Config::default(),
    );
    {
        let mut pin = pin!(&mut behavior);
        assert!(poll_immediate(poll_fn(|cx| pin.poll(cx))).await.is_none());
    }
    let address = Multiaddr::empty();
    let role_override = Endpoint::Dialer;
    behavior.on_swarm_event(libp2p::swarm::FromSwarm::ConnectionEstablished(
        ConnectionEstablished {
            peer_id: PEER_ID.parse().unwrap(),
            connection_id: ConnectionId::new_unchecked(0),
            endpoint: &ConnectedPoint::Dialer {
                address,
                role_override,
            },
            failed_addresses: &[],
            other_established: 0,
        },
    ));
    let mut pin = pin!(&mut behavior);
    assert!(poll_immediate(poll_fn(|cx| pin.poll(cx))).await.is_none());
}

// #[cfg(loomtest)]
mod loomtest {
    use super::*;
    use crate::libp2p::{ConnectionInfo, Event, PeerInfo};
    use libp2p::swarm::ToSwarm;

    #[test]
    fn behaviour_poll() {
        loom::model(|| {
            let mut behavior = Behaviour::new(
                MockReconForInterest::new(),
                MockReconForEventId::new(),
                Config::default(),
            );
            let res = loom::future::block_on(poll_immediate(poll_fn(|cx| {
                behavior.poll(cx)
            })));
            assert!(res.is_none())
        });
    }

    #[test]
    fn behaviour_poll_with_peer() {
        loom::model(|| {
            let mut behavior = Behaviour::new(
                MockReconForInterest::default(),
                MockReconForEventId::default(),
                Config::default(),
            );
            behavior.peers.insert(
                PEER_ID.parse().unwrap(),
                PeerInfo {
                    status: PeerStatus::Waiting,
                    connections: vec![ConnectionInfo {
                        id: ConnectionId::new_unchecked(0),
                        dialer: true,
                    }],
                    last_sync: None,
                },
            );
            let res = loom::future::block_on(poll_immediate(poll_fn(|cx| {
                behavior.poll(cx)
            })));
            assert!(res.is_none());
        });
    }

    #[test]
    fn behaviour_send() {
        loom::model(|| {
            let mut behavior = Behaviour::new(
                MockReconForInterest::default(),
                MockReconForEventId::default(),
                Config::default(),
            );
            let mut behavior = std::pin::Pin::new(&mut behavior);
            let res = loom::future::block_on(poll_immediate(poll_fn(|cx| {
                behavior.poll(cx)
            })));
            assert!(res.is_none());

            behavior.send_event(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                remote_peer_id: PEER_ID.parse().unwrap(),
                status: PeerStatus::Waiting,
            })));

            let res = loom::future::block_on(poll_immediate(poll_fn(|cx| {
                behavior.poll(cx)
            })));

            assert!(res.is_some());
        });
    }
}
