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

use ceramic_core::Bytes;
use libp2p::swarm::{keep_alive, Swarm, SwarmEvent};
use libp2p_swarm_test::SwarmExt;
use quickcheck::QuickCheck;
use std::{
    num::NonZeroU8,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::debug;
use tracing_test::traced_test;

use crate::{
    libp2p::{Behaviour, Config, Event, PeerStatus},
    BTreeStore, Recon, Sha256a,
};

type ReconTest = Recon<Bytes, Sha256a, BTreeStore<Bytes, Sha256a>>;

#[test]
#[traced_test]
fn recon_sync() {
    // Synchronizing Recon should be invariant to the number of times we drive the swarms.
    // Driving a swarm effectively starts a new sync.
    fn prop(drive_count: NonZeroU8) {
        debug!("count: {drive_count}");
        let config = Config {
            // Immediately start a new sync once the previous has finished.
            per_peer_sync_timeout: Duration::from_millis(0),
            ..Default::default()
        };
        let mut swarm1 = Swarm::new_ephemeral(|_| {
            Behaviour::new(
                Arc::new(Mutex::new(ReconTest::new(BTreeStore::from_set(
                    [Bytes::from("swarm1")].into(),
                )))),
                config.clone(),
            )
        });
        let mut swarm2 = Swarm::new_ephemeral(|_| {
            Behaviour::new(
                Arc::new(Mutex::new(ReconTest::new(BTreeStore::from_set(
                    [Bytes::from("swarm2")].into(),
                )))),
                config,
            )
        });

        async_std::task::block_on(async {
            swarm1.listen().await;
            swarm2.connect(&mut swarm1).await;

            for _ in 0..drive_count.get() {
                debug!("start drive");
                let (swarm1_events, swarm2_events) =
                    match libp2p_swarm_test::drive(&mut swarm1, &mut swarm2).await {
                        (
                            [Event::PeerEvent {
                                remote_peer_id: p1_started,
                                status: PeerStatus::Started,
                                ..
                            }, Event::PeerEvent {
                                remote_peer_id: p1_synced,
                                status: PeerStatus::Synchronized,
                            }],
                            [Event::PeerEvent {
                                remote_peer_id: p2_started,
                                status: PeerStatus::Started,
                                ..
                            }, Event::PeerEvent {
                                remote_peer_id: p2_synced,
                                status: PeerStatus::Synchronized,
                            }],
                        ) => ((p1_started, p1_synced), (p2_started, p2_synced)),
                        events => panic!("unexpected swarm events {events:?}"),
                    };

                debug!("drive finished");

                // Assert both events are about the same peer id, per swarm
                assert_eq!(swarm1_events.0, swarm1_events.1);
                assert_eq!(swarm2_events.0, swarm2_events.1);

                // Assert that swarms have synchronized with the opposite peers
                assert_eq!(&swarm1_events.0, swarm2.local_peer_id());
                assert_eq!(&swarm2_events.0, swarm1.local_peer_id());
            }
        });
    }

    QuickCheck::new().tests(10).quickcheck(prop as fn(_))
}

#[test]
#[traced_test]
fn unsupported_doesnt_fail() {
    let mut swarm1 = Swarm::new_ephemeral(|_| keep_alive::Behaviour);
    let mut swarm2 = Swarm::new_ephemeral(|_| {
        Behaviour::new(
            Arc::new(Mutex::new(ReconTest::new(BTreeStore::default()))),
            Config::default(),
        )
    });

    let result = async_std::task::block_on(async {
        swarm1.listen().await;
        swarm2.connect(&mut swarm1).await;
        async_std::task::spawn(swarm1.loop_on_next());

        loop {
            match swarm2.next_swarm_event().await {
                SwarmEvent::Behaviour(Event::PeerEvent {
                    status: PeerStatus::Stopped,
                    ..
                }) => break Ok(()),
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
