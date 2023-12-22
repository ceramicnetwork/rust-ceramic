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

use cid::{
    multihash::{Hasher, Sha2_256},
    Cid,
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::{Eq, Ord},
    fmt::Display,
};
use unsigned_varint::{decode::u64 as de_varint, encode::u64 as varint};

use crate::network::Network;

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

    /// try to parse a CID out of a CIP-124 EventID
    pub fn cid(&self) -> Option<Cid> {
        let (streamid, remainder) = de_varint(&self.0).unwrap_or_default();
        if streamid != 0xce {
            return None; // not a streamid
        };

        let (typeid, remainder) = de_varint(remainder).unwrap_or_default();
        if typeid != 0x05 {
            return None; // not a CIP-124 EventID
        };

        let (_network_id, mut remainder) = de_varint(remainder).unwrap_or_default();

        // strip separator [u8; 8] controller [u8; 8] StreamID [u8; 4]
        remainder = &remainder[(8 + 8 + 4)..];

        // height cbor unsigned integer
        if remainder[0] <= 23 {
            // 0 - 23
            remainder = &remainder[1..]
        } else if remainder[0] == 24 {
            // u8
            remainder = &remainder[2..]
        } else if remainder[0] == 25 {
            // u16
            remainder = &remainder[3..]
        } else if remainder[0] == 26 {
            // u32
            remainder = &remainder[5..]
        } else if remainder[0] == 27 {
            // u64
            remainder = &remainder[9..]
        } else {
            // not a cbor unsigned int
            return None;
        };
        match Cid::read_bytes(remainder) {
            Ok(v) => Some(v),
            Err(_) => None, // not a CID
        }
    }
}

impl std::fmt::Debug for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_tuple("EventId").field(&self.0).finish()
        } else {
            let bytes = self.as_slice();
            if bytes.len() < 6 {
                write!(f, "{}", hex::encode_upper(bytes))
            } else {
                write!(f, "{}", hex::encode_upper(&bytes[bytes.len() - 6..]))
            }
        }
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode_upper(self.as_slice()))
    }
}

impl From<&[u8]> for EventId {
    fn from(bytes: &[u8]) -> Self {
        EventId(bytes.to_owned())
    }
}

impl From<Vec<u8>> for EventId {
    fn from(bytes: Vec<u8>) -> Self {
        EventId(bytes)
    }
}
impl From<&Vec<u8>> for EventId {
    fn from(bytes: &Vec<u8>) -> Self {
        EventId(bytes.to_owned())
    }
}

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
    // TODO sort_value should be bytes not str
    pub fn with_sort_value(mut self, sort_key: &str, sort_value: &str) -> Builder<WithSortValue> {
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
    pub fn with_min_controller(mut self) -> Builder<WithController> {
        self.state.bytes.extend(ZEROS_8);
        Builder {
            state: WithController {
                bytes: self.state.bytes,
            },
        }
    }
    pub fn with_max_controller(mut self) -> Builder<WithController> {
        self.state.bytes.extend(FFS_8);
        Builder {
            state: WithController {
                bytes: self.state.bytes,
            },
        }
    }
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
    pub fn with_min_init(mut self) -> Builder<WithInit> {
        self.state.bytes.extend(ZEROS_4);
        Builder {
            state: WithInit {
                bytes: self.state.bytes,
            },
        }
    }
    pub fn with_max_init(mut self) -> Builder<WithInit> {
        self.state.bytes.extend(FFS_4);
        Builder {
            state: WithInit {
                bytes: self.state.bytes,
            },
        }
    }
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
    pub fn with_min_event_height(mut self) -> Builder<WithEventHeight> {
        // 0x00 is the cbor encoding of 0.
        self.state.bytes.push(0x00);
        Builder {
            state: WithEventHeight {
                bytes: self.state.bytes,
            },
        }
    }
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
    /// Builds the final EventId as a fencepost
    pub fn build_fencepost(self) -> EventId {
        EventId(self.state.bytes)
    }
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
            EventId(
                [
                    206,
                    1,
                    5,
                    0,
                    126,
                    113,
                    14,
                    33,
                    127,
                    160,
                    226,
                    89,
                    69,
                    204,
                    124,
                    7,
                    47,
                    247,
                    41,
                    234,
                    104,
                    59,
                    117,
                    23,
                    24,
                    255,
                    1,
                    113,
                    18,
                    32,
                    244,
                    239,
                    126,
                    194,
                    8,
                    148,
                    77,
                    37,
                    112,
                    37,
                    64,
                    139,
                    182,
                    71,
                    148,
                    158,
                    107,
                    114,
                    147,
                    5,
                    32,
                    188,
                    128,
                    243,
                    77,
                    139,
                    251,
                    175,
                    210,
                    100,
                    61,
                    134,
                ],
            )
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
}
