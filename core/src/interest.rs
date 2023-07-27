use anyhow::Result;
use cid::multihash::{Hasher, Sha2_256};
use minicbor::{Decoder, Encoder};
use multibase::Base;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

pub use libp2p_identity::PeerId;

use crate::RangeOpen;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Interest declares an interest in a keyspace for a given peer.
/// cbor([sort_key, peer_id, start_key, stop_key, not after])
pub struct Interest(#[serde(with = "serde_bytes")] Vec<u8>);

impl Interest {
    /// Create a builder for constructing EventIds.
    pub fn builder() -> Builder<Init> {
        Builder { state: Init }
    }

    /// Report the sort key
    pub fn sort_key_hash(&self) -> Result<&[u8]> {
        let mut decoder = Decoder::new(&self.0);
        Ok(decoder.bytes()?)
    }

    /// Report the PeerId value
    pub fn peer_id(&self) -> Result<PeerId> {
        let mut decoder = Decoder::new(&self.0);
        // Skip sort key
        decoder.skip()?;
        let peer_id_bytes = decoder.bytes()?;
        Ok(PeerId::from_bytes(peer_id_bytes)?)
    }

    /// Report the range value
    pub fn range(&self) -> Result<RangeOpen<Vec<u8>>> {
        let mut decoder = Decoder::new(&self.0);
        // Skip sort key
        decoder.skip()?;
        // Skip peer_id
        decoder.skip()?;
        let start = decoder.bytes()?.to_vec();
        let end = decoder.bytes()?.to_vec();
        Ok(RangeOpen { start, end })
    }

    /// Report the not after value
    pub fn not_after(&self) -> Result<u64> {
        let mut decoder = Decoder::new(&self.0);
        // Skip sort key
        decoder.skip()?;
        // Skip peer_id
        decoder.skip()?;
        // Skip start_key
        decoder.skip()?;
        // Skip end_key
        decoder.skip()?;

        Ok(decoder.u64()?)
    }

    /// Return the interest as a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Display for Interest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", multibase::encode(Base::Base58Btc, &self.0))
    }
}
impl FromStr for Interest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (_, bytes) = multibase::decode(s)?;
        // TODO validate these bytes are valid Interest bytes
        Ok(Self(bytes))
    }
}

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

/// Builder with sort key set
pub struct WithSortKey {
    encoder: Encoder<Vec<u8>>,
}
impl BuilderState for WithSortKey {}

/// Builder with peer id set
pub struct WithPeerId {
    encoder: Encoder<Vec<u8>>,
}
impl BuilderState for WithPeerId {}

/// Builder with range key set
pub struct WithRange {
    encoder: Encoder<Vec<u8>>,
}
impl BuilderState for WithRange {}

/// Builder with not after set
pub struct WithNotAfter {
    encoder: Encoder<Vec<u8>>,
}
impl BuilderState for WithNotAfter {}

impl Builder<Init> {
    pub fn with_sort_key(self, sort_key: &str) -> Builder<WithSortKey> {
        // A typical interest contains:
        //
        // - sort_key : 8 bytes
        // - peer_id: ~66 bytes
        // - start_key: ~72 bytes
        // - end_key: ~72 bytes
        // - not_after: ~8 bytes
        //
        // with some cbor overhead that is about 256 bytes
        //
        // TODO: Emperically measure performance of this size.
        const INITIAL_VEC_CAPACITY: usize = 256;
        let mut hasher = Sha2_256::default();
        hasher.update(sort_key.as_bytes());
        // sha256 is 32 bytes safe to unwrap to [u8; 32]
        let hash: [u8; 32] = hasher.finalize().try_into().unwrap();
        let mut encoder = Encoder::new(Vec::with_capacity(INITIAL_VEC_CAPACITY));
        encoder
            // Encode last 8 bytes of the sort_key hash
            .bytes(&hash[hash.len() - 8..])
            .expect("sort_key should cbor encode");
        Builder {
            state: WithSortKey { encoder },
        }
    }
}
impl Builder<WithSortKey> {
    pub fn with_peer_id(mut self, peer_id: &PeerId) -> Builder<WithPeerId> {
        self.state
            .encoder
            .bytes(&peer_id.to_bytes())
            .expect("peer_id should cbor encode");
        Builder {
            state: WithPeerId {
                encoder: self.state.encoder,
            },
        }
    }
}
impl Builder<WithPeerId> {
    pub fn with_range<'a>(mut self, range: impl Into<RangeOpen<&'a [u8]>>) -> Builder<WithRange> {
        let range = range.into();
        self.state
            .encoder
            .bytes(range.start)
            .expect("start_key should cbor encode")
            .bytes(range.end)
            .expect("end_key should cbor encode");
        Builder {
            state: WithRange {
                encoder: self.state.encoder,
            },
        }
    }
}
impl Builder<WithRange> {
    pub fn with_not_after(mut self, not_after: u64) -> Builder<WithNotAfter> {
        self.state
            .encoder
            .u64(not_after)
            .expect("not_after should cbor encode");
        Builder {
            state: WithNotAfter {
                encoder: self.state.encoder,
            },
        }
    }
}

impl Builder<WithNotAfter> {
    pub fn build(self) -> Interest {
        Interest(self.state.encoder.into_writer())
    }
}

impl From<&[u8]> for Interest {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_owned())
    }
}

impl From<Vec<u8>> for Interest {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}
impl From<&Vec<u8>> for Interest {
    fn from(bytes: &Vec<u8>) -> Self {
        Self(bytes.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use expect_test::expect;

    use super::*;

    #[test]
    fn roundtrip() {
        let peer_id = PeerId::from_str("1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ").unwrap();
        let interest = Interest::builder()
            .with_sort_key("model")
            .with_peer_id(&peer_id)
            .with_range((&[0, 1, 2][..], &[0, 1, 9][..]))
            .with_not_after(0)
            .build();
        expect!["z6oybn5exQL8GmN4GWBvRVZx5hbxgPMKwrhU7bsYzgwYJs9ygqJDriYNxfuxXdBHFV1vuURkiWK"]
            .assert_eq(&interest.to_string());

        assert_eq!(interest, Interest::from_str(&interest.to_string()).unwrap());
    }

    #[test]
    fn accessors() {
        let peer_id = PeerId::from_str("1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ").unwrap();
        let interest = Interest::builder()
            .with_sort_key("model")
            .with_peer_id(&peer_id)
            .with_range((&[0x00, 0x01, 0x02][..], &[0x00, 0x01, 0x09][..]))
            .with_not_after(123456789)
            .build();

        expect![[r#"
            "0F70D652B6B825E4"
        "#]]
        .assert_debug_eq(&hex::encode_upper(interest.sort_key_hash().unwrap()));
        expect![[r#"
            "1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ"
        "#]]
        .assert_debug_eq(&interest.peer_id().unwrap().to_string());
        expect![[r#"
            RangeOpen {
                start: [
                    0,
                    1,
                    2,
                ],
                end: [
                    0,
                    1,
                    9,
                ],
            }
        "#]]
        .assert_debug_eq(&interest.range().unwrap());
        expect![[r#"
            123456789
        "#]]
        .assert_debug_eq(&interest.not_after().unwrap());
    }
}
