use anyhow::{anyhow, Result};
use cbor::{CborBytes, Decoder, Encoder};
use cid::multihash::{Hasher, Sha2_256};
use multibase::Base;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, io::Cursor, ops::Range, str::FromStr};

pub use libp2p_identity::PeerId;

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
    pub fn sort_key(&self) -> Result<String> {
        let mut decoder = Decoder::from_bytes(self.0.as_slice());
        let sort_key_bytes = decode_bytes(&mut decoder)?;
        Ok(String::from_utf8(sort_key_bytes)?)
    }

    /// Report the PeerId value
    pub fn peer_id(&self) -> Result<PeerId> {
        let mut decoder = Decoder::from_bytes(self.0.as_slice());
        let peer_id_bytes = decode_bytes(&mut decoder)?;
        Ok(PeerId::from_bytes(&peer_id_bytes)?)
    }

    /// Report the range value
    /// TODO do not use Range type as it uses an inclusive lower bound, we want exclusive bounds.
    pub fn range(&self) -> Result<Range<Vec<u8>>> {
        let mut decoder = Decoder::from_bytes(self.0.as_slice());
        // Skip sort key
        decoder.items().next();
        // Skip peer_id
        decoder.items().next();
        let start = decode_bytes(&mut decoder)?;
        let end = decode_bytes(&mut decoder)?;
        Ok(Range { start, end })
    }

    /// Report the not after value
    pub fn not_after(&self) -> Result<u64> {
        let mut decoder = Decoder::from_bytes(self.0.as_slice());
        // Skip peer_id
        decoder.items().next();
        // Skip start
        decoder.items().next();
        // Skip end
        decoder.items().next();

        decode_u64(&mut decoder)
    }

    /// Return the interest as a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

fn decode_bytes(decoder: &mut Decoder<Cursor<Vec<u8>>>) -> Result<Vec<u8>> {
    if let Some(item) = decoder.items().next() {
        let item = item?;
        match item {
            cbor::Cbor::Bytes(data) => Ok(data.0),
            item => Err(anyhow!("expected cbor bytes, found: {:?}", item)),
        }
    } else {
        Err(anyhow!("expected top level cbor value, found nothing"))
    }
}
fn decode_u64(decoder: &mut Decoder<Cursor<Vec<u8>>>) -> Result<u64> {
    if let Some(item) = decoder.items().next() {
        let item = item?;
        match item {
            cbor::Cbor::Unsigned(data) => Ok(data.into_u64()),
            item => Err(anyhow!("expected cbor unsigned integer, found: {:?}", item)),
        }
    } else {
        Err(anyhow!("expected top level cbor value, found nothing"))
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
        let mut hasher = Sha2_256::default();
        hasher.update(sort_key.as_bytes());
        // sha256 is 32 bytes safe to unwrap to [u8; 32]
        let hash: [u8; 32] = hasher.finalize().try_into().unwrap();
        let mut encoder = Encoder::from_memory();
        encoder
            // Encode last 8 bytes of the sort_key hash
            .encode([CborBytes(hash[hash.len() - 8..].to_vec())])
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
            .encode([CborBytes(peer_id.to_bytes())])
            .expect("peer id should cbor encode");
        Builder {
            state: WithPeerId {
                encoder: self.state.encoder,
            },
        }
    }
}
impl Builder<WithPeerId> {
    pub fn with_range(mut self, range: Range<&[u8]>) -> Builder<WithRange> {
        self.state
            .encoder
            .encode([
                CborBytes(range.start.to_vec()),
                CborBytes(range.end.to_vec()),
            ])
            .expect("peer id should cbor encode");
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
            .encode([not_after])
            .expect("not after should cbor encode");
        Builder {
            state: WithNotAfter {
                encoder: self.state.encoder,
            },
        }
    }
}

impl Builder<WithNotAfter> {
    pub fn build(mut self) -> Interest {
        Interest(self.state.encoder.as_bytes().to_vec())
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
            .with_range(&[0, 1, 2]..&[0, 1, 9])
            .with_not_after(0)
            .build();
        expect!["z6oybn5exQL8GmN4GWBvRVZx5hbxgPMKwrhU7bsYzgwYJs9ygqJDriYNxfuxXdBHFV1vuURkiWK"]
            .assert_eq(&interest.to_string());

        assert_eq!(interest, Interest::from_str(&interest.to_string()).unwrap());
    }

    #[test]
    fn range() {
        let peer_id = PeerId::from_str("1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ").unwrap();
        let interest = Interest::builder()
            .with_sort_key("model")
            .with_peer_id(&peer_id)
            .with_range(&[0x00, 0x01, 0x02]..&[0x00, 0x01, 0x09])
            .with_not_after(0)
            .build();

        expect![[r#"
            [
                0,
                1,
                2,
            ]..[
                0,
                1,
                9,
            ]
        "#]]
        .assert_debug_eq(&interest.range().unwrap());
    }
}
