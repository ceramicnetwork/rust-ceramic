//! Interest is a structure that declares a range of data in which a node is interested.
use anyhow::Result;
use minicbor::{Decoder, Encoder};
use multibase::Base;
use multihash_codetable::Sha2_256;
use multihash_derive::Hasher;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};

pub use libp2p_identity::PeerId;

use crate::{EventId, RangeOpen};

const MIN_BYTES: [u8; 0] = [];
const MAX_BYTES: [u8; 1] = [0xFF];

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Interest declares an interest in a keyspace for a given peer.
/// cbor seq:
///     sep_key, the stream set identifier e.g. "model"
///     peer_id,  the libp2p address of the interested node
///     indicator, an indicator of whether we have a min(0), range(1), or max(2)
///     start_key, Inclusive start of the range
///     stop_key, Exclusive end of the range
///     not after, Expiration of the interest or 0 to indicate indefinite
pub struct Interest(#[serde(with = "serde_bytes")] Vec<u8>);

impl Interest {
    /// Create a builder for constructing EventIds.
    pub fn builder() -> Builder<Init> {
        Builder { state: Init }
    }

    /// Report the sort key
    pub fn sep_key_hash(&self) -> Option<&[u8]> {
        Some(self.as_parts()?.sep_key_hash)
    }

    /// Report the PeerId value
    pub fn peer_id(&self) -> Option<PeerId> {
        PeerId::from_bytes(self.as_parts()?.peer_id).ok()
    }

    /// Report the range value.
    /// Returns None if no range can be decoded.
    pub fn range(&self) -> Option<RangeOpen<Vec<u8>>> {
        self.as_parts()?.range.map(|(start, end)| RangeOpen {
            start: start.to_vec(),
            end: end.to_vec(),
        })
    }

    /// Report the not after value
    pub fn not_after(&self) -> Option<u64> {
        self.as_parts()?.not_after
    }

    /// Return the interest as a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    // Parses the interest into its parts
    // A valid interest may not contain all parts.
    // However a None from this method indicates an invalid interest.
    fn as_parts(&self) -> Option<InterestParts<'_>> {
        let mut decoder = Decoder::new(&self.0);
        let sep_key_hash = decoder.bytes().ok()?;
        let peer_id = decoder.bytes().ok()?;
        let indicator = decoder.u8().ok()?;
        let (range, not_after) = if indicator != 1 {
            (None, None)
        } else {
            (
                Some((decoder.bytes().ok()?, decoder.bytes().ok()?)),
                Some(decoder.u64().ok()?),
            )
        };
        Some(InterestParts {
            sep_key_hash,
            peer_id,
            range,
            not_after,
        })
    }
}

struct InterestParts<'a> {
    sep_key_hash: &'a [u8],
    peer_id: &'a [u8],
    range: Option<(&'a [u8], &'a [u8])>,
    not_after: Option<u64>,
}

impl std::fmt::Debug for Interest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            if self.0 == MIN_BYTES {
                f.debug_struct("Interest").field("bytes", &"MIN").finish()
            } else if self.0 == MAX_BYTES {
                f.debug_struct("Interest").field("bytes", &"MAX").finish()
            } else {
                f.debug_struct("Interest")
                    .field("bytes", &hex::encode(&self.0))
                    .field(
                        "sep_key_hash",
                        &hex::encode(self.sep_key_hash().ok_or(std::fmt::Error)?),
                    )
                    .field("peer_id", &self.peer_id().ok_or(std::fmt::Error)?)
                    .field(
                        "range",
                        // NOTE: This is a bit of a hack. Interests are designed to range over
                        // arbitrary bytes so decoding the range as EventId is not generally
                        // correct. However interests only range over EventIds in the current
                        // implementation. When we get a second type we can update this code
                        // accordingly. In the meantime decoding EventIds is very helpful for
                        // debugging.
                        &self.range().ok_or(std::fmt::Error)?.map(EventId::try_from),
                    )
                    .field("not_after", &self.not_after().ok_or(std::fmt::Error)?)
                    .finish()
            }
        } else {
            write!(f, "{}", hex::encode_upper(self.as_slice()))
        }
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
    /// Builds the minimum possible Interest
    pub fn build_min_fencepost(self) -> Interest {
        Interest(MIN_BYTES.into())
    }
    /// Builds the maximum possible Interest
    pub fn build_max_fencepost(self) -> Interest {
        Interest(MAX_BYTES.into())
    }
    /// Builds an interest starting with a specific sort key.
    pub fn with_sep_key(self, sep_key: &str) -> Builder<WithSortKey> {
        // A typical interest contains:
        //
        // - sep_key: 8 bytes
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
        hasher.update(sep_key.as_bytes());
        // sha256 is 32 bytes safe to unwrap to [u8; 32]
        let hash: [u8; 32] = hasher.finalize().try_into().unwrap();
        let mut encoder = Encoder::new(Vec::with_capacity(INITIAL_VEC_CAPACITY));
        encoder
            // Encode last 8 bytes of the sep_key hash
            .bytes(&hash[hash.len() - 8..])
            .expect("sep_key should cbor encode");
        Builder {
            state: WithSortKey { encoder },
        }
    }
}
impl Builder<WithSortKey> {
    /// Builds interest with specific peer id.
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
    /// Builds interest with minimum possible range.
    pub fn with_min_range(mut self) -> Builder<WithRange> {
        self.state.encoder.u8(0).expect("min range should encode");
        Builder {
            state: WithRange {
                encoder: self.state.encoder,
            },
        }
    }
    /// Builds interest with max possible range.
    pub fn with_max_range(mut self) -> Builder<WithRange> {
        self.state.encoder.u8(2).expect("min range should encode");
        Builder {
            state: WithRange {
                encoder: self.state.encoder,
            },
        }
    }
    /// Builds interest with specific range.
    pub fn with_range<'a>(mut self, range: impl Into<RangeOpen<&'a [u8]>>) -> Builder<WithRange> {
        let range = range.into();
        self.state
            .encoder
            .u8(1)
            .expect("range start indicator should cbor encode")
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
    /// Builds interest as fencepost.
    pub fn build_fencepost(self) -> Interest {
        Interest(self.state.encoder.into_writer())
    }
    /// Builds interest with specific not_after value
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
    /// Builds a complete interest.
    pub fn build(self) -> Interest {
        Interest(self.state.encoder.into_writer())
    }
}

impl TryFrom<Vec<u8>> for Interest {
    type Error = InvalidInterest;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let interest = Self(value);
        if interest.0 == MIN_BYTES || interest.0 == MAX_BYTES {
            // We have a min or max interest which is valid but does not parse
            Ok(interest)
        } else {
            // Parse the interest to ensure its valid
            if interest.as_parts().is_some() {
                Ok(interest)
            } else {
                Err(InvalidInterest(interest.0))
            }
        }
    }
}

/// Error when constructing an interest.
/// Holds the bytes of the invalid interest.
pub struct InvalidInterest(pub Vec<u8>);

impl std::fmt::Debug for InvalidInterest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("InvalidInterest")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl std::fmt::Display for InvalidInterest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid interest bytes")
    }
}
impl std::error::Error for InvalidInterest {}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use expect_test::expect;

    use super::*;

    #[test]
    fn roundtrip() {
        let peer_id = PeerId::from_str("1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ").unwrap();
        let interest = Interest::builder()
            .with_sep_key("model")
            .with_peer_id(&peer_id)
            .with_range((&[0, 1, 2][..], &[0, 1, 9][..]))
            .with_not_after(0)
            .build();
        expect!["zSeLpVyYXXyR6aowRRqCo76S7kDJkqeid8Mcg8v8DdHjBrNdCpB1kSMVvikk6AcgWg7rTgQ7ocLP"]
            .assert_eq(&interest.to_string());

        assert_eq!(interest, Interest::from_str(&interest.to_string()).unwrap());
    }

    #[test]
    fn accessors() {
        let peer_id = PeerId::from_str("1AZtAkWrrQrsXMQuBEcBget2vGAPbdQ2Wn4bESe9QEVypJ").unwrap();
        let interest = Interest::builder()
            .with_sep_key("model")
            .with_peer_id(&peer_id)
            .with_range((&[0x00, 0x01, 0x02][..], &[0x00, 0x01, 0x09][..]))
            .with_not_after(123456789)
            .build();

        expect![[r#"
            "0F70D652B6B825E4"
        "#]]
        .assert_debug_eq(&hex::encode_upper(interest.sep_key_hash().unwrap()));
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
