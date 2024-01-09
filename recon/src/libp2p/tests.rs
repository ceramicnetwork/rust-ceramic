// Copyright 2019 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Integration tests for the `Ping` network behaviour.

use ceramic_core::{Cid, EventId, Interest, Network, PeerId};
use cid::multihash::{Code, MultihashDigest};
use libp2p::{
    ping,
    swarm::{Swarm, SwarmEvent},
};
use libp2p_swarm_test::SwarmExt;
use prometheus_client::registry::Registry;
use quickcheck::QuickCheck;
use rand::{thread_rng, Rng};
use std::{num::NonZeroU8, time::Duration};
use tokio::runtime::Runtime;
use tracing::debug;
use tracing_test::traced_test;

use crate::{
    libp2p::{stream_set::StreamSet, Behaviour, Config, Event, PeerEvent, PeerStatus},
    recon::{FullInterests, ReconInterestProvider},
    BTreeStore, Client, Metrics, Recon, Server, Sha256a,
};

type InterestStore = BTreeStore<Interest, Sha256a>;
type InterestInterest = FullInterests<Interest>;
type ReconInterest = Recon<Interest, Sha256a, InterestStore, InterestInterest>;
type ReconInterestClient = Client<Interest, Sha256a>;

type ModelStore = BTreeStore<EventId, Sha256a>;
type ModelInterest = ReconInterestProvider;
type ReconModel = Recon<EventId, Sha256a, ModelStore, ModelInterest>;
type ReconModelClient = Client<EventId, Sha256a>;

type SwarmTest = Swarm<Behaviour<ReconInterestClient, ReconModelClient>>;

fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    thread_rng().fill(&mut data);
    let hash = Code::Sha2_256.digest(&data);
    Cid::new_v1(0x00, hash)
}

// Build an ephemeral swarm for testing.
//
// Using the property based testing means we have multipleS tokio runtimes.
// So we expect to be passed the runtime directly instead of assuming a singleton runtime.
fn build_swarm(runtime: &Runtime, name: &str, config: Config) -> SwarmTest {
    Swarm::new_ephemeral(|identity| {
        let peer_id = PeerId::from(identity.public());
        let mut interest = Server::new(ReconInterest::new(
            BTreeStore::from_set(
                [Interest::builder()
                    .with_sort_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((&[][..], &[0xFF; 1][..]))
                    .with_not_after(100)
                    .build()]
                .into(),
            ),
            FullInterests::default(),
            Metrics::register(&mut Registry::default()),
        ));

        let mut model = Server::new(ReconModel::new(
            BTreeStore::from_set(
                [
                    // Initialize with three events
                    EventId::new(
                        &Network::Mainnet,
                        "model",
                        &format!("{}_model", name),
                        &format!("{}_controller", name),
                        &random_cid(),
                        10,
                        &random_cid(),
                    ),
                    EventId::new(
                        &Network::Mainnet,
                        "model",
                        &format!("{}_model", name),
                        &format!("{}_controller", name),
                        &random_cid(),
                        10,
                        &random_cid(),
                    ),
                    EventId::new(
                        &Network::Mainnet,
                        "model",
                        &format!("{}_model", name),
                        &format!("{}_controller", name),
                        &random_cid(),
                        10,
                        &random_cid(),
                    ),
                ]
                .into(),
            ),
            ModelInterest::new(peer_id, interest.client()),
            Metrics::register(&mut Registry::default()),
        ));
        let b = Behaviour::new(interest.client(), model.client(), config.clone());
        runtime.spawn(interest.run());
        runtime.spawn(model.run());
        b
    })
}

#[test]
#[traced_test]
fn recon_sync() {
    // Synchronizing Recon should be invariant to the number of times we drive the swarms.
    // Driving a swarm effectively starts a new sync.
    fn prop(drive_count: NonZeroU8) {
        // Explicitly create tokio runtime.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        debug!("count: {drive_count}");
        let config = Config {
            // Immediately start a new sync once the previous has finished.
            per_peer_sync_timeout: Duration::from_millis(0),
            ..Default::default()
        };
        let mut swarm1 = build_swarm(&runtime, "swarm1", config.clone());
        let mut swarm2 = build_swarm(&runtime, "swarm2", config);

        runtime.block_on(async {
            swarm1.listen().with_memory_addr_external().await;
            swarm2.connect(&mut swarm1).await;

            for _ in 0..drive_count.get() {
                debug!("start drive");
                let (swarm1_events, swarm2_events) = match libp2p_swarm_test::drive(
                    &mut swarm1,
                    &mut swarm2,
                )
                .await
                {
                    (
                        // Sequence of events from swarm1
                        [Event::PeerEvent(swarm1_event0), Event::PeerEvent(swarm1_event1), Event::PeerEvent(swarm1_event2), Event::PeerEvent(swarm1_event3)],
                        // Sequence of events from swarm2
                        [Event::PeerEvent(swarm2_event0), Event::PeerEvent(swarm2_event1), Event::PeerEvent(swarm2_event2), Event::PeerEvent(swarm2_event3)],
                    ) => (
                        (swarm1_event0, swarm1_event1, swarm1_event2, swarm1_event3),
                        (swarm2_event0, swarm2_event1, swarm2_event2, swarm2_event3),
                    ),
                };

                debug!("drive finished");

                // Assert all events are about the same peer id, per swarm
                assert_eq!(
                    swarm1_events.0.remote_peer_id,
                    swarm1_events.1.remote_peer_id
                );
                assert_eq!(
                    swarm1_events.0.remote_peer_id,
                    swarm1_events.2.remote_peer_id
                );
                assert_eq!(
                    swarm1_events.0.remote_peer_id,
                    swarm1_events.3.remote_peer_id
                );

                assert_eq!(
                    swarm2_events.0.remote_peer_id,
                    swarm2_events.1.remote_peer_id
                );
                assert_eq!(
                    swarm2_events.0.remote_peer_id,
                    swarm2_events.2.remote_peer_id
                );
                assert_eq!(
                    swarm2_events.0.remote_peer_id,
                    swarm2_events.3.remote_peer_id
                );

                // Assert that swarms have synchronized with the opposite peers
                assert_eq!(&swarm1_events.0.remote_peer_id, swarm2.local_peer_id());
                assert_eq!(&swarm2_events.0.remote_peer_id, swarm1.local_peer_id());

                // Assert event0 status
                assert_eq!(
                    PeerStatus::Started {
                        stream_set: StreamSet::Interest
                    },
                    swarm1_events.0.status
                );
                assert_eq!(
                    PeerStatus::Started {
                        stream_set: StreamSet::Interest
                    },
                    swarm2_events.0.status
                );

                // Assert event1 status
                assert_eq!(
                    PeerStatus::Synchronized {
                        stream_set: StreamSet::Interest
                    },
                    swarm1_events.1.status
                );
                assert_eq!(
                    PeerStatus::Synchronized {
                        stream_set: StreamSet::Interest
                    },
                    swarm2_events.1.status
                );

                // Assert event2 status
                assert_eq!(
                    PeerStatus::Started {
                        stream_set: StreamSet::Model
                    },
                    swarm1_events.2.status
                );
                assert_eq!(
                    PeerStatus::Started {
                        stream_set: StreamSet::Model
                    },
                    swarm2_events.2.status
                );

                // Assert event3 status
                assert_eq!(
                    PeerStatus::Synchronized {
                        stream_set: StreamSet::Model
                    },
                    swarm1_events.3.status
                );
                assert_eq!(
                    PeerStatus::Synchronized {
                        stream_set: StreamSet::Model
                    },
                    swarm2_events.3.status
                );
            }
        });
    }

    QuickCheck::new().tests(10).quickcheck(prop as fn(_))
}

#[test]
#[traced_test]
fn unsupported_does_not_fail() {
    // Explicitly create tokio runtime.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut swarm1 = Swarm::new_ephemeral(|_| ping::Behaviour::default());
    let mut swarm2 = build_swarm(&runtime, "swarm2", Config::default());
    let result = runtime.block_on(async {
        swarm1.listen().with_memory_addr_external().await;
        swarm2.connect(&mut swarm1).await;
        runtime.spawn(swarm1.loop_on_next());

        loop {
            match swarm2.next_swarm_event().await {
                SwarmEvent::Behaviour(Event::PeerEvent(PeerEvent {
                    status: PeerStatus::Stopped,
                    ..
                })) => break Ok(()),
                SwarmEvent::ConnectionClosed { cause: Some(e), .. } => {
                    break Err(e);
                }
                SwarmEvent::ConnectionClosed { cause: None, .. } => {
                    break Ok(());
                }
                _ => {}
            }
        }
    });

    result
        .expect("node with recon protocol should not fail connection due to unsupported protocol");
}
