//! EventId generates EventIDs from event data.
//!
//! varint(0xce) + // streamid, 2 bytes b'\xce\x01'
//! varint(0x05) + // cip-124_EventID, 1 byte b'\x05'
//! varint(networkId), // networkId 1 byte wellknown or 5 bytes for local network
//! last8Bytes(sha256(separator_key + "|" + separator_value)), // separator 16 bytes
//! context // first 8 bytes provided by application, 0 pad at end to 8 bytes, truncate to 8 bytes
//! cbor(blockNumber), // u64_max 9 bytes
//! last8Bytes(sha256(stream_controller_DID)), // controller 8 bytes
//! last4Bytes(init_event_CID) // stream 4 bytes
//! eventCID // mostly 36 bytes but could be inline CID
//! 2 + 1 + 5 + 8 + 8 + 9 + 8 + 4 + 36 = 81
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

        // strip separator [u8; 8] context [u8; 8]
        remainder = &remainder[(8 + 8)..];

        // blockNumber cbor unsigned integer
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

        // strip controller [u8; 8] StreamID [u8; 4]
        remainder = &remainder[(8 + 4)..];

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
            write!(f, "{}", hex::encode_upper(self.as_slice()))
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

fn pad8_bytes(buf: &[u8]) -> [u8; 8] {
    let mut bytes: [u8; 8] = [0; 8];
    let length = if buf.len() > 8 { 8 } else { buf.len() };
    bytes[..length].copy_from_slice(&buf[..length]);
    bytes
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

/// Builder with context set.
#[derive(Debug)]
pub struct WithContext {
    bytes: Vec<u8>,
}
impl BuilderState for WithContext {}

/// Builder with context set.
#[derive(Debug)]
pub struct WithBlockNumber {
    bytes: Vec<u8>,
}
impl BuilderState for WithBlockNumber {}

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
    pub fn with_network(self, network: &Network) -> Builder<WithNetwork> {
        //! varint(0xce) + // streamid, 2 bytes b'\xce\x01'
        //! varint(0x05) + // cip-124_EventID, 1 byte b'\x05'
        //! varint(networkId), // networkId 1 byte wellknown or 5 bytes for local network

        let mut bytes = Vec::with_capacity(128);
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
    // last8Bytes(sha256(separator_key + "|" + separator_value)), // separator 16 bytes
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
    // context // first 8 bytes provided by application, 0 pad at end to 8 bytes, truncate to 8 bytes
    pub fn with_context(mut self, context: &str) -> Builder<WithContext> {
        self.state
            .bytes
            .extend(last8_bytes(&pad8_bytes(context.as_bytes())));
        Builder {
            state: WithContext {
                bytes: self.state.bytes,
            },
        }
    }
}
impl Builder<WithContext> {
    // cbor(blockNumber), // u64_max 9 bytes
    pub fn with_blocknumber(mut self, blocknumber: u64) -> Builder<WithBlockNumber> {
        let blocknumber_cbor = minicbor::to_vec(blocknumber).unwrap();
        self.state.bytes.extend(blocknumber_cbor);
        Builder {
            state: WithBlockNumber {
                bytes: self.state.bytes,
            },
        }
    }

    pub fn with_max_blocknumber_fencepost(mut self) -> EventId {
        self.state.bytes.extend(&[0xFF; 1]); // 0xff is grater then all CBOR numbers
        EventId(self.state.bytes)
    }
}
impl Builder<WithBlockNumber> {
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
    // last8Bytes(sha256(stream_controller_DID)), // controller 8 bytes
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
    // last4Bytes(init_event_CID) // stream 4 bytes
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

impl Builder<WithController> {
    // last4Bytes(init_event_CID) // stream 4 bytes
    pub fn with_event(mut self, init: &Cid) -> Builder<WithInit> {
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
    /// Builds the final EventId as a fencepost
    pub fn build_fencepost(self) -> EventId {
        EventId(self.state.bytes)
    }
    // eventCID // mostly 36 bytes but could be inline CID
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
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let eid = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
        expect![[
            "fce0105007e710e217fa0e25900000000000000001a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"
        ]]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::builder()
            .with_network(&Network::TestnetClay)
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
        expect!["fce0105017e710e217fa0e25900000000000000001a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::builder()
            .with_network(&Network::DevUnstable)
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
        expect!["fce0105027e710e217fa0e25900000000000000001a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::builder()
            .with_network(&Network::InMemory)
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
        expect!["fce0105ff017e710e217fa0e25900000000000000001a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::builder()
            .with_network(&Network::Local(0xce4a441c))
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
        expect!["fce01059c88a9f21c7e710e217fa0e25900000000000000001a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));
    }

    #[test]
    fn test_serialize() {
        let sort_key = "model".to_string();
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let received = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_context("0123456789")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();

        let received_cbor = hex::encode(serde_ipld_dagcbor::ser::to_vec(&received).unwrap());
        println!("serde_json {}", serde_json::to_string(&received).unwrap()); // Message as json
        expect![[
            "5849ce0105007e710e217fa0e25930313233343536371a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"
        ]].assert_eq(&received_cbor);
    }

    #[test]
    fn test_deserialize() {
        let bytes = hex::decode("5849ce0105007e710e217fa0e25930313233343536371a011d06b045cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86").unwrap();
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
                48,
                49,
                50,
                51,
                52,
                53,
                54,
                55,
                26,
                1,
                29,
                6,
                176,
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
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_context("0123456789")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .with_event(&event_cid)
            .build();
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
            .with_context("1234567890")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .build_fencepost();
        assert_eq!(None, event_id.cid());

        // Min event height
        let event_id = EventId::builder()
            .with_network(&Network::Mainnet)
            .with_sort_value(&sort_key, &separator)
            .with_context("")
            .with_blocknumber(18679472)
            .with_controller(&controller)
            .with_init(&init)
            .build_fencepost();
        assert_eq!(None, event_id.cid());
    }
}
