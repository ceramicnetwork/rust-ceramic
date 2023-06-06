#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use cbor::Encoder;
use cid::Cid;
use multihash::{Hasher, Sha2_256};
use unsigned_varint::encode::u64 as varint;
/// EventId generates EventIDs from event data.
///
/// varint(0xce) + // streamid, 1 byte
/// varint(0x05) + // cip-124 EventID, 1 byte
/// varint(networkId), // 1 byte (5 for local network)
/// last8Bytes(sha256(separator_value)), // 16 bytes
/// last8Bytes(sha256(stream_controller_DID)), // 16 bytes
/// last4Bytes(init_event_CID) // 8 bytes
/// cbor(eventHeight), // 1-3 bytes
/// eventCID // 36 bytes
///   0x01 cidv1, 1 byte
///   0x71 dag-cbor, 1 byte
///   0x12 sha2-256, 1byte
///   0x20 varint(hash length), 1 byte
///   hash bytes, 32 bytes

/// Network values from https://cips.ceramic.network/tables/networkIds.csv
/// Ceramic Pubsub Topic, Timestamp Authority
#[derive(Debug)]
pub enum Network {
    /// /ceramic/mainnet Ethereum Mainnet (EIP155:1)
    Mainnet,

    /// /ceramic/testnet-clay Ethereum Gnosis Chain
    TestnetClay,

    /// /ceramic/dev-unstable Ethereum Gnosis Chain
    DevUnstable,

    /// /ceramic/local-$(randomNumber) Ethereum by Truffle Ganache
    Local(u32),

    /// None
    InMemory,
}

impl Network {
    /// network.to_u64() to get the network as a u64
    pub fn to_u64(&self) -> u64 {
        match self {
            // https://github.com/ceramicnetwork/CIPs/blob/main/tables/networkIds.csv
            Network::Mainnet => 0x00,
            Network::TestnetClay => 0x01,
            Network::DevUnstable => 0x02,
            Network::InMemory => 0xff,
            Network::Local(id) => 0x01_0000_0000_u64 + u64::from(*id),
        }
    }
}

#[derive(Debug)]
/// EventId is the event data as a recon key
pub struct EventId(Vec<u8>);

impl EventId {
    /// EventId.new builds a Vec<u8> with the event id data.
    pub fn new(
        network_id: Network,
        separator: &str,
        controller: &str,
        init: &Cid,
        event_height: u64,
        event_cid: &Cid,
    ) -> EventId {
        let mut event_height_cbor = Encoder::from_memory();
        event_height_cbor.encode([event_height]).unwrap();
        EventId(
            [
                varint(0xce, &mut [0_u8; 10]),                // streamid varint
                varint(0x05, &mut [0_u8; 10]),                // cip-124 EventID varint
                varint(network_id.to_u64(), &mut [0_u8; 10]), // network_id varint
                last8_bytes(&sha256_digest(separator)),       // separator [u8; 8]
                last8_bytes(&sha256_digest(controller)),      // controller [u8; 8]
                last4_bytes(init.to_bytes().as_slice()),      // StreamID [u8; 4]
                event_height_cbor.as_bytes(),                 // event_height cbor unsigned int
                //varint(event_height, &mut [0_u8; 10]), // event_height varint
                event_cid.to_bytes().as_slice(), // [u8]
            ]
            .concat(),
        )
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

#[cfg(test)]
mod tests {
    use crate::eventid::{Cid, EventId, Network};
    use cid::multibase::{self, Base};
    use expect_test::expect;
    use std::str::FromStr;

    #[test]
    fn blessing() {
        let separator = "kh4q0ozorrgaq2mezktnrmdwleo1d".to_string(); // cspell:disable-line
        let controller = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9".to_string();
        let init =
            Cid::from_str("bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq").unwrap(); // cspell:disable-line
        let event_height = 255; // so we get 2 bytes b'\x18\xff'
        let event_cid =
            Cid::from_str("bafyreihu557meceujusxajkaro3epfe6nnzjgbjaxsapgtml7ox5ezb5qy").unwrap(); // cspell:disable-line

        let eid = EventId::new(
            Network::Mainnet,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            "fce0105002a30541a6fbdca4645cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"
        ]]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            Network::TestnetClay,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            r#"fce0105012a30541a6fbdca4645cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"#
        ]]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            Network::DevUnstable,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            r#"fce0105022a30541a6fbdca4645cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"#
        ]]
        .assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            Network::InMemory,
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            r#"fce0105ff012a30541a6fbdca4645cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"#
        ]].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));

        let eid = EventId::new(
            Network::Local(0xce4a441c),
            &separator,
            &controller,
            &init,
            event_height,
            &event_cid,
        );
        expect![[
            r#"fce01059c88a9f21c2a30541a6fbdca4645cc7c072ff729ea683b751718ff01711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86"#
        ]].assert_eq(&multibase::encode(Base::Base16Lower, eid.0));
    }
}
