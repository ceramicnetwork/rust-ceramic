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
    ///     "kh4q0ozorrgaq2mezktnrmdwleo1d",
    ///     "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9",
    ///     &Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(),
    ///     1,
    ///     &Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(),
    /// );
    ///
    /// ```
    pub fn new(
        network: &Network,
        sort_key: &str,
        sort_value: &str,
        controller: &str,
        init: &Cid,
        event_height: u64,
        event_cid: &Cid,
    ) -> EventId {
        EventId::builder()
            .with_network(network)
            .with_sort_value(sort_key, sort_value)
            .with_controller(controller)
            .with_init(init)
            .with_event_height(event_height)
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
    /// Report the event height of the EventId
    pub fn event_height(&self) -> Option<u64> {
        self.as_parts()?.height
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

        let (height, cid) = cbor_uint_decode(&remainder[STREAM_ID_RANGE.end..]);

        Some(EventIdParts {
            network_id,
            separator,
            controller,
            stream_id,
            height,
            cid,
        })
    }
}
// Decode a cbor unsigned integer and return the remaining bytes from the buffer
fn cbor_uint_decode(data: &[u8]) -> (Option<u64>, &[u8]) {
    // From the spec: https://datatracker.ietf.org/doc/html/rfc7049#section-2.1
    //
    // Major type 0:  an unsigned integer.  The 5-bit additional information
    //  is either the integer itself (for additional information values 0
    //  through 23) or the length of additional data.  Additional
    //  information 24 means the value is represented in an additional
    //  uint8_t, 25 means a uint16_t, 26 means a uint32_t, and 27 means a
    //  uint64_t.  For example, the integer 10 is denoted as the one byte
    //  0b000_01010 (major type 0, additional information 10).  The
    //  integer 500 would be 0b000_11001 (major type 0, additional
    //  information 25) followed by the two bytes 0x01f4, which is 500 in
    //  decimal.

    match data[0] {
        // 0 - 23
        x if x <= 23 => (Some(x as u64), &data[1..]),
        // u8
        24 => (
            data[1..2]
                .try_into()
                .ok()
                .map(|h| u8::from_be_bytes(h) as u64),
            &data[2..],
        ),
        // u16
        25 => (
            data[1..3]
                .try_into()
                .ok()
                .map(|h| u16::from_be_bytes(h) as u64),
            &data[3..],
        ),
        // u32
        26 => (
            data[1..5]
                .try_into()
                .ok()
                .map(|h| u32::from_be_bytes(h) as u64),
            &data[5..],
        ),
        // u64
        27 => (
            data[1..9].try_into().ok().map(u64::from_be_bytes),
            &data[9..],
        ),
        // not a cbor unsigned int
        _ => (None, data),
    }
}

struct EventIdParts<'a> {
    network_id: u64,
    separator: &'a [u8],
    controller: &'a [u8],
    stream_id: &'a [u8],
    height: Option<u64>,
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
                    .field("event_height", &self.event_height())
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
pub struct WithSortValue {
    bytes: Vec<u8>,
}
impl BuilderState for WithSortValue {}

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

/// Builder with event height set.
#[derive(Debug)]
pub struct WithEventHeight {
    bytes: Vec<u8>,
}
impl BuilderState for WithEventHeight {}

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
    pub fn with_sort_value(mut self, sort_key: &str, sort_value: &str) -> Builder<WithSortValue> {
        // TODO sort_value should be bytes not str
        self.state.bytes.extend(last8_bytes(&sha256_digest(&format!(
            "{}|{}",
            sort_key, sort_value,
        ))));
        Builder {
            state: WithSortValue {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithSortValue> {
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
    /// Specify that the minimum event height should be used for the event
    pub fn with_min_event_height(mut self) -> Builder<WithEventHeight> {
        // 0x00 is the cbor encoding of 0.
        self.state.bytes.push(0x00);
        Builder {
            state: WithEventHeight {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify that the maximum event height should be used for the event
    pub fn with_max_event_height(mut self) -> Builder<WithEventHeight> {
        // 0xFF is the break stop code in CBOR, and will sort higher than any cbor encoded unsigned
        // integer.
        self.state.bytes.push(0xFF);
        Builder {
            state: WithEventHeight {
                bytes: self.state.bytes,
            },
        }
    }
    /// Specify event height for the event
    pub fn with_event_height(mut self, event_height: u64) -> Builder<WithEventHeight> {
        let event_height_cbor = minicbor::to_vec(event_height).unwrap();
        // event_height cbor unsigned int
        self.state.bytes.extend(event_height_cbor);
        Builder {
            state: WithEventHeight {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithEventHeight> {
    /// Builds the final EventId as a fencepost.
    /// A fencepost is a value that sorts before and after specific events but is itself not a
    /// complete EventId.
    pub fn build_fencepost(self) -> EventId {
        EventId(self.state.bytes)
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
    use test_log::test;

    #[test]
    fn blessing() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_height = 255; // so we get 2 bytes b'\x18\xff'
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let eid = EventId::new(
            &Network::Mainnet,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            "fce0105007e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"
        ]]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::TestnetClay,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect!["fce0105017e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::DevUnstable,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect!["fce0105027e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::InMemory,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect!["fce0105ff017e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            &Network::Local(0xce4a441c),
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect!["fce01059c88a9f21c7e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));
    }

    #[test]
    fn test_serialize() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_height = 255; // so we get 2 bytes b'\x18\xff'
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let received = EventId::new(
            &Network::Mainnet,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );

        let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
        println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
        expect![[
            "583ece0105007e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"
        ]].assert_eq(&received_cbor);
    }

    #[test]
    fn test_deserialize() {
        let bytes = hex::decode("583ece0105007e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86").unwrap();
        let x = serde_ipld_dagcbor::de::from_slice(bytes.as_slice());
        let received: EventId = x.unwrap();
        let cid = received.cid();
        println!("{:?}, {:?}", &received, &cid);
        expect![[r#"
            EventId {
                bytes: "ce0105007e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86",
                network_id: Some(
                    0,
                ),
                separator: Some(
                    "7e710e217fa0e259",
                ),
                controller: Some(
                    "45cc7c072ff729ea",
                ),
                stream_id: Some(
                    "683b7517",
                ),
                event_height: Some(
                    255,
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
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_height = 255; // so we get 2 bytes b'\x18\xff'
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let event_id = EventId::new(
            &Network::Mainnet,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        assert_eq!(Some(event_cid), event_id.cid());
    }
    #[test]
    fn no_cid() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line

        // Max event height
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_controller(&controller)
            .with_init(&init)
            .with_max_event_height()
            .build_fencepost();
        assert_eq!(None, event_id.cid());

        // Min event height
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_controller(&controller)
            .with_init(&init)
            .with_min_event_height()
            .build_fencepost();
        assert_eq!(None, event_id.cid());
    }
    #[test]
    fn debug() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_height = 255; // so we get 2 bytes b'\x18\xff'
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let event_id = EventId::new(
            &Network::TestnetClay,
            &sort_key,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[r#"
            EventId {
                bytes: "ce0105017e710e217fa0e25945cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86",
                network_id: Some(
                    1,
                ),
                separator: Some(
                    "7e710e217fa0e259",
                ),
                controller: Some(
                    "45cc7c072ff729ea",
                ),
                stream_id: Some(
                    "683b7517",
                ),
                event_height: Some(
                    255,
                ),
                cid: Some(
                    "bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy",
                ),
            }
        "#]]
        .assert_debug_eq(&event_id);
    }
    #[test]
    fn event_height() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        for event_height in [
            1,
            18,
            255,
            256,
            65535,
            65536,
            4294967295,
            4294967296,
            18446744073709551615,
        ] {
            let event_id = EventId::new(
                &Network::TestnetClay,
                &sort_key,
                &separator,
                &controller,
                &init,
                event_height,
                &event_cid,
            );
            assert_eq!(Some(event_height), event_id.event_height());
        }
    }
}
