//! EventId generates EventIDs from event data.
//!
//! varint(0xce) + // streamid, 1 byte
//! varint(0x05) + // cip-124 EventID, 1 byte
//! varint(networkId), // 1 byte (5 for local network)
//! last8Bytes(sha256(separator_key + "|" + separator_value)), // 16 bytes
//! last8Bytes(sha256(stream_controller_DID)), // 16 bytes
//! last4Bytes(init_event_CID) // 8 bytes
//! cbor(eventHeight), // 1-3 bytes
//! eventCID // 36 bytes
//!   0x01 cidv1, 1 byte
//!   0x71 dag-cbor, 1 byte
//!   0x12 sha2-256, 1byte
//!   0x20 varint(hash length), 1 byte
//!   hash bytes, 32 bytes
#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use cid::Cid;
use multihash_codetable::Sha2_256;
use multihash_derive::Hasher;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{Eq, Ord},
    fmt::Display,
    ops::Range,
};
use unsigned_varint::{decode::u64 as de_varint, encode::u64 as varint};

use crate::network::Network;

const MIN_BYTES: [u8; 0] = [];
const MAX_BYTES: [u8; 1] = [0xFF];

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// EventId is the event data as a recon key
pub struct EventId(#[serde(with = "serde_bytes")] Vec<u8>);

impl EventId {
    /// Create a builder for constructing EventIds.
    pub fn builder() -> Builder<Init> {
        Builder { state: Init }
    }

    ///  EventId.new builds a Vec<u8> with the event id data.
    /// ## Example
    /// ```
    /// use std::str::FromStr;
    /// use cid::Cid;
    /// use ceramic_core::{EventId, Network};
    ///
    /// let event = EventId::new(
    ///     &Network::Mainnet,
    ///     "model",
    ///     &multibase::decode("kh4q0ozorrgaq2mezktnrmdwleo1d").unwrap().1,
    ///     "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9",
    ///     &Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(),
    ///     &Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(),
    /// );
    ///
    /// ```
    pub fn new(
        network: &Network,
        sep_key: &str,
        sep_value: &[u8],
        controller: &str,
        init: &Cid,
        event_cid: &Cid,
    ) -> EventId {
        EventId::builder()
            .with_network(network)
            .with_sep(sep_key, sep_value)
            .with_controller(controller)
            .with_init(init)
            .with_event(event_cid)
            .build()
    }

    /// Extract the raw bytes from an EventId
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_owned()
    }
    /// Expose the raw bytes of the EventId
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Report the network id of the EventId
    pub fn network_id(&self) -> Option<u64> {
        self.as_parts().map(|parts| parts.network_id)
    }
    /// Report the separator bytes of the EventId
    pub fn separator(&self) -> Option<&[u8]> {
        self.as_parts().map(|parts| parts.separator)
    }
    /// Report the controller bytes of the EventId
    pub fn controller(&self) -> Option<&[u8]> {
        self.as_parts().map(|parts| parts.controller)
    }
    /// Report the stream_id bytes of the EventId
    pub fn stream_id(&self) -> Option<&[u8]> {
        self.as_parts().map(|parts| parts.stream_id)
    }

    /// Report the event CID of the EventId
    pub fn cid(&self) -> Option<Cid> {
        let parts = self.as_parts()?;
        Cid::read_bytes(parts.cid).ok()
    }

    fn as_parts(&self) -> Option<EventIdParts<'_>> {
        let (streamid, remainder) = de_varint(&self.0).unwrap_or_default();
        if streamid != 0xce {
            return None; // not a streamid
        };

        let (typeid, remainder) = de_varint(remainder).unwrap_or_default();
        if typeid != 0x05 {
            return None; // not a CIP-124 EventID
        };

        let (network_id, remainder) = de_varint(remainder).unwrap_or_default();

        // separator [u8; 8]
        const SEPARATOR_RANGE: Range<usize> = 0..8;
        // controller [u8; 8]
        const CONTROLLER_RANGE: Range<usize> = 8..(8 + 8);
        // StreamID [u8; 4]
        const STREAM_ID_RANGE: Range<usize> = (8 + 8)..(8 + 8 + 4);

        let separator = &remainder[SEPARATOR_RANGE];
        let controller = &remainder[CONTROLLER_RANGE];
        let stream_id = &remainder[STREAM_ID_RANGE];

        let cid = &remainder[STREAM_ID_RANGE.end..];

        Some(EventIdParts {
            network_id,
            separator,
            controller,
            stream_id,
            cid,
        })
    }

    /// swap_cid replaces the event CID in the EventId with a new CID.
    pub fn swap_cid(&self, cid: &Cid) -> Option<Self> {
        // Strip the stream ID multiformat and stream type
        let remainder = &self.0[3..];
        // Strip and decode the network ID (variable length)
        let (network_id, remainder) = de_varint(remainder).ok()?;

        let mut bytes: Vec<u8> = Default::default();
        // Add stream ID multiformat and the stream type as defined in the cip-124 EventID varint
        bytes.extend(b"\xce\x01\x05");
        // network_id varint
        bytes.extend(varint(network_id, &mut [0_u8; 10]));
        // separator 8 bytes, controller 8 bytes, stream_id 4 bytes
        bytes.extend(&remainder[0..(8 + 8 + 4)]);
        bytes.extend(cid.to_bytes());
        Some(Self(bytes))
    }
}

struct EventIdParts<'a> {
    network_id: u64,
    separator: &'a [u8],
    controller: &'a [u8],
    stream_id: &'a [u8],
    cid: &'a [u8],
}

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            if self.0 == MIN_BYTES {
                f.debug_struct("EventId").field("bytes", &"MIN").finish()
            } else if self.0 == MAX_BYTES {
                f.debug_struct("EventId").field("bytes", &"MAX").finish()
            } else {
                f.debug_struct("EventId")
                    .field("bytes", &hex::encode(&self.0))
                    .field("network_id", &self.network_id())
                    .field("separator", &self.separator().map(hex::encode))
                    .field("controller", &self.controller().map(hex::encode))
                    .field("stream_id", &self.stream_id().map(hex::encode))
                    .field("cid", &self.cid().map(|cid| cid.to_string()))
                    .finish()
            }
        } else {
            write!(f, "{}", hex::encode_upper(self.as_slice()))
        }
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.as_slice()))
    }
}

impl TryFrom<Vec<u8>> for EventId {
    type Error = InvalidEventId;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let event_id = Self(bytes);
        if event_id.0 == MIN_BYTES || event_id.0 == MAX_BYTES {
            // We have a min or max event id which is valid but does not parse
            Ok(event_id)
        } else {
            // Parse the event id to ensure its valid
            if event_id.as_parts().is_some() {
                Ok(event_id)
            } else {
                Err(InvalidEventId(event_id.0))
            }
        }
    }
}

/// Error when constructing an event id.
/// Holds the bytes of the invalid event id.
pub struct InvalidEventId(pub Vec<u8>);

impl std::fmt::Debug for InvalidEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("InvalidEventId")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl std::fmt::Display for InvalidEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid event id bytes")
    }
}
impl std::error::Error for InvalidEventId {}

fn sha256_digest(s: &str) -> [u8; 32] {
    let mut hasher = Sha2_256::default();
    hasher.update(s.as_bytes());
    // sha256 is 32 bytes safe to unwrap to [u8; 32]
    hasher.finalize().try_into().unwrap()
}

fn last8_bytes(buf: &[u8]) -> &[u8] {
    &buf[(buf.len() - 8)..]
}

fn last4_bytes(buf: &[u8]) -> &[u8] {
    &buf[(buf.len() - 4)..]
}

const ZEROS_8: &[u8] = &[0; 8];
const FFS_8: &[u8] = &[0xFF; 8];
const ZEROS_4: &[u8] = &[0; 4];
const FFS_4: &[u8] = &[0xFF; 4];

/// Builder provides an ordered API for constructing an EventId
#[derive(Debug)]
pub struct Builder<S: BuilderState> {
    state: S,
}
/// The state of the builder
pub trait BuilderState {}

/// Initial state of the builder.
#[derive(Debug)]
pub struct Init;
impl BuilderState for Init {}

/// Builder with network set.
#[derive(Debug)]
pub struct WithNetwork {
    bytes: Vec<u8>,
}
impl BuilderState for WithNetwork {}

/// Builder with sort value set.
#[derive(Debug)]
pub struct WithSep {
    bytes: Vec<u8>,
}
impl BuilderState for WithSep {}

/// Builder with controller set.
#[derive(Debug)]
pub struct WithController {
    bytes: Vec<u8>,
}
impl BuilderState for WithController {}

/// Builder with init event CID set.
#[derive(Debug)]
pub struct WithInit {
    bytes: Vec<u8>,
}
impl BuilderState for WithInit {}

/// Builder with event CID set.
#[derive(Debug)]
pub struct WithEvent {
    bytes: Vec<u8>,
}
impl BuilderState for WithEvent {}

impl Builder<Init> {
    /// Builds the minimum possible EventId
    pub fn build_min_fencepost(self) -> EventId {
        EventId(MIN_BYTES.into())
    }
    /// Builds the maximum possible EventId
    pub fn build_max_fencepost(self) -> EventId {
        EventId(MAX_BYTES.into())
    }
    /// Specify the network of the event
    pub fn with_network(self, network: &Network) -> Builder<WithNetwork> {
        // Maximum EventId size is 72.
        //
        // varint(0xce) + // streamid, 2 bytes b'\xce\x01'
        // varint(0x05) + // cip-124 EventID, 1 byte b'\x05'
        // varint(networkId), // 1-5 bytes for local network
        // last8Bytes(sha256(separator_key + "|" + separator_value)), // 16 bytes
        // last8Bytes(sha256(stream_controller_DID)), // 8 bytes
        // last4Bytes(init_event_CID) // 4 bytes
        // cbor(eventHeight), // u64_max 9 bytes
        // eventCID // mostly 36 bytes but could be inline CID

        let mut bytes = Vec::with_capacity(72);
        // streamid varint
        bytes.extend(varint(0xce, &mut [0_u8; 10]));
        // cip-124 EventID varint
        bytes.extend(varint(0x05, &mut [0_u8; 10]));
        // network_id varint
        bytes.extend(varint(network.id(), &mut [0_u8; 10]));
        Builder {
            state: WithNetwork { bytes },
        }
    }
}
impl Builder<WithNetwork> {
    /// Specify the sort key and value of the event
    pub fn with_sep(mut self, sep_key: &str, sep_value: &[u8]) -> Builder<WithSep> {
        let mut hasher = Sha2_256::default();
        hasher.update(sep_key.as_bytes());
        hasher.update(sep_value);

        self.state.bytes.extend(last8_bytes(hasher.finalize()));
        Builder {
            state: WithSep {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithSep> {
    /// Specify that the minimum controller value should be used for the event
    pub fn with_min_controller(mut self) -> Builder<WithController> {
        self.state.bytes.extend(ZEROS_8);
        Builder {
            state: WithController {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify that the maximum controller value should be used for the event
    pub fn with_max_controller(mut self) -> Builder<WithController> {
        self.state.bytes.extend(FFS_8);
        Builder {
            state: WithController {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify the controller for the event
    pub fn with_controller(mut self, controller: &str) -> Builder<WithController> {
        self.state
            .bytes
            .extend(last8_bytes(&sha256_digest(controller)));
        Builder {
            state: WithController {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithController> {
    /// Specify that the minimum init value should be used for the event
    pub fn with_min_init(mut self) -> Builder<WithInit> {
        self.state.bytes.extend(ZEROS_4);
        Builder {
            state: WithInit {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify that the maximum init value should be used for the event
    pub fn with_max_init(mut self) -> Builder<WithInit> {
        self.state.bytes.extend(FFS_4);
        Builder {
            state: WithInit {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify the init cid of the event
    pub fn with_init(mut self, init: &Cid) -> Builder<WithInit> {
        self.state
            .bytes
            .extend(last4_bytes(init.to_bytes().as_slice()));
        Builder {
            state: WithInit {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithInit> {
    /// Specify that the minimum event CID should be used for the event
    pub fn with_min_event(self) -> Builder<WithEvent> {
        Builder {
            state: WithEvent {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify that the maximum event CID should be used for the event
    pub fn with_max_event(mut self) -> Builder<WithEvent> {
        self.state.bytes.extend(&[0xFF]);
        Builder {
            state: WithEvent {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify the event cid
    pub fn with_event(mut self, event: &Cid) -> Builder<WithEvent> {
        self.state.bytes.extend(event.to_bytes());
        Builder {
            state: WithEvent {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithEvent> {
    /// Builds the final EventId as a fencepost.
    /// A fencepost is a value that sorts before and after specific events but is itself not a
    /// complete EventId.
    pub fn build_fencepost(self) -> EventId {
        EventId(self.state.bytes)
    }
    /// Builds the final EventId
    pub fn build(self) -> EventId {
        EventId(self.state.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::multibase::{self, Base};
    use expect_test::expect;
    use std::str::FromStr;
    use test_pretty_log::test;

    #[test]
    fn blessing() {
        let sep_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let eid = EventId::new(
            &Network::Mainnet,
            &sep_key,
            &multibase::decode(&separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect!["fce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::TestnetClay,
            &sep_key,
            &multibase::decode(&separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect!["fce010501ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::DevUnstable,
            &sep_key,
            &multibase::decode(&separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect!["fce010502ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::InMemory,
            &sep_key,
            &multibase::decode(&separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect!["fce0105ff01ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::Local(0xce4a441c),
            &sep_key,
            &multibase::decode(&separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect!["fce01059c88a9f21cba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));
    }

    #[test]
    fn test_serialize() {
        let sep_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let received = EventId::new(
            &Network::Mainnet,
            &sep_key,
            &multibase::decode(separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );

        let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
        println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
        expect!["583cce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&received_cbor);
    }

    #[test]
    fn test_deserialize() {
        let bytes = hex::decode("583cce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86").unwrap();
        let x = serde_ipld_dagcbor::de::from_slice(bytes.as_slice());
        let received: EventId = x.unwrap();
        let cid = received.cid();
        println!("{:?}, {:?}", &received, &cid);
        expect![[r#"
            EventId {
                bytes: "ce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86",
                network_id: Some(
                    0,
                ),
                separator: Some(
                    "ba25076d730241e7",
                ),
                controller: Some(
                    "45cc7c072ff729ea",
                ),
                stream_id: Some(
                    "683b7517",
                ),
                cid: Some(
                    "bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy",
                ),
            }
        "#]]
        .assert_debug_eq(&received);

        expect![["bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy"]] // cspell:disable-line
            .assert_eq(cid.unwrap().to_string().as_str());
    }
    #[test]
    fn cid() {
        let sepy = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let event_id = EventId::new(
            &Network::Mainnet,
            &sepy,
            &multibase::decode(separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        assert_eq!(Some(event_cid), event_id.cid());
    }
    #[test]
    fn no_cid() {
        let sep_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line

        // Max event height
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sep(&sep_key, &multibase::decode(&separator).unwrap().1)
            .with_controller(&controller)
            .with_init(&init)
            .with_max_event()
            .build_fencepost();
        assert_eq!(None, event_id.cid());

        // Min event height
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sep(&sep_key, &multibase::decode(&separator).unwrap().1)
            .with_controller(&controller)
            .with_init(&init)
            .with_min_event()
            .build_fencepost();
        assert_eq!(None, event_id.cid());
    }
    #[test]
    fn debug() {
        let sep_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let event_id = EventId::new(
            &Network::TestnetClay,
            &sep_key,
            &multibase::decode(separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );
        expect![[r#"
            EventId {
                bytes: "ce010501ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86",
                network_id: Some(
                    1,
                ),
                separator: Some(
                    "ba25076d730241e7",
                ),
                controller: Some(
                    "45cc7c072ff729ea",
                ),
                stream_id: Some(
                    "683b7517",
                ),
                cid: Some(
                    "bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy",
                ),
            }
        "#]]
        .assert_debug_eq(&event_id);
    }

    #[test]
    fn test_swap_cid() {
        let sep_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let original = EventId::new(
            &Network::Mainnet,
            &sep_key,
            &multibase::decode(separator).unwrap().1,
            &controller,
            &init,
            &event_cid,
        );

        println!("original: {:?}", hex::encode(&original.0));
        // Test replacing the CID in the EventId with one that is of a different length
        let swapped = original.swap_cid(&init).unwrap();
        assert_ne!(original, swapped);

        // Test replacing the CID in the EventID with the original CID
        let swapped_back = swapped.swap_cid(&event_cid).unwrap();
        assert_eq!(original, swapped_back);
    }
}
