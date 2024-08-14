use std::fmt::Display;
use std::{fs, path::PathBuf, str::FromStr};

use crate::{StreamId, StreamIdType};
use anyhow::{anyhow, Context, Ok, Result};
use cid::multihash::Multihash;
use cid::Cid;
use libp2p_identity::PeerId;
use rand::Rng;
use ring::signature::{Ed25519KeyPair, KeyPair};

const ED25519_MULTICODEC: u64 = 0xed;
const ED25519_PUBLIC_KEY_MULTICODEC_PREFIX: &[u8; 2] = b"\xed\x01";
const ED25519_PRIVATE_KEY_MULTICODEC_PREFIX: &[u8; 2] = b"\x80\x26";
const ED25519_LIBP2P_PEER_ID_PREFIX: &[u8; 4] = b"\x08\x01\x12\x20";

/// NodeId is the public_ed25519_key_bytes of the node
#[derive(Clone, Eq, PartialEq, Copy)]
pub struct NodeId {
    public_ed25519_key_bytes: [u8; 32],
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did_key())
    }
}

impl NodeId {
    /// public_ed25519_key_bytes as a CID
    pub fn cid(&self) -> Cid {
        let hash = Multihash::<64>::wrap(0, self.public_ed25519_key_bytes.as_slice())
            .expect("ed25519 public key is 32 bytes");
        Cid::new_v1(ED25519_MULTICODEC, hash)
    }
    /// public_ed25519_key_bytes as a StreamId
    pub fn stream_id(&self) -> StreamId {
        StreamId {
            r#type: StreamIdType::Unloadable,
            cid: self.cid(),
        }
    }
    /// public_ed25519_key_bytes as a did:key
    pub fn did_key(&self) -> String {
        let public_with_prefix = [
            ED25519_PUBLIC_KEY_MULTICODEC_PREFIX,
            self.public_ed25519_key_bytes.as_ref(),
        ]
        .concat();
        let public_multibase = multibase::encode(multibase::Base::Base58Btc, public_with_prefix);
        format!("did:key:{}", public_multibase)
    }
    /// public_ed25519_key_bytes as a PeerID
    pub fn peer_id(&self) -> PeerId {
        let libp2p_key = [
            ED25519_LIBP2P_PEER_ID_PREFIX,
            self.public_ed25519_key_bytes.as_slice(),
        ]
        .concat();
        // Identity multihash code = 0x00
        let libp2p_key_multihash = Multihash::<64>::wrap(0x00, &libp2p_key)
            .expect("self.public_ed25519_key_bytes to be well formed");
        PeerId::from_multihash(libp2p_key_multihash)
            .expect("self.public_ed25519_key_bytes to be well formed")
    }
    /// public_ed25519_key_bytes from a Cid
    pub fn try_from_cid(cid: Cid) -> Result<Self> {
        let mh = cid.hash();
        if mh.code() != 0x00 {
            return Err(anyhow!("Cid multihash is not identity"));
        }
        if mh.size() != 32 {
            return Err(anyhow!("CID multihash is not 36 bytes"));
        }
        Ok(Self {
            public_ed25519_key_bytes: mh.digest().try_into()?,
        })
    }
    /// public_ed25519_key_bytes from a did:key
    pub fn try_from_did_key(did_key: &str) -> Result<Self> {
        let public_key_multibase = did_key
            .strip_prefix("did:key:")
            .context("DID did not start with did:key")?;
        let ed25519_public_key_with_prefix: [u8; 34] = multibase::decode(public_key_multibase)?
            .1
            .try_into()
            .map_err(|_| anyhow!("Failed to decode public key multibase"))?;
        let ed25519_public_key = ed25519_public_key_with_prefix
            .strip_prefix(ED25519_PUBLIC_KEY_MULTICODEC_PREFIX)
            .context("")?;
        let public_ed25519_key_bytes: [u8; 32] = ed25519_public_key.try_into()?;
        Ok(Self {
            public_ed25519_key_bytes,
        })
    }
    /// Read an Ed25519 key from a directory and return a NodeID with a key pair
    pub fn try_from_dir(key_dir: PathBuf) -> Result<(Self, Ed25519KeyPair)> {
        let key_path = key_dir.join("id_ed25519_0");
        let content = fs::read_to_string(key_path)?;
        let seed = ssh_key::private::PrivateKey::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse private key: {}", e))?
            .key_data()
            .ed25519()
            .map_or(Err(anyhow::anyhow!("failed to parse ed25519 key")), |key| {
                Ok(key.private.to_bytes())
            })?;
        let key_pair = Ed25519KeyPair::from_seed_unchecked(seed.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to create key pair: {}", e))?;
        let public_ed25519_key_bytes = key_pair.public_key().as_ref().try_into()?;
        Ok((
            Self {
                public_ed25519_key_bytes,
            },
            key_pair,
        ))
    }
    /// Create an Ed25519 key pair from a secret. The secret can be formatted in two ways:
    /// - Multibase of Secret:Multibase of Public Key
    ///   (e.g. z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M)
    ///   In this example, the DID will be did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M.
    ///
    /// - Multibase of unchecked Secret (i.e. not matched against public key)
    ///   (e.g. z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd)
    pub fn try_from_secret(secret: &str) -> Result<(Self, Ed25519KeyPair)> {
        let mut parts = secret.split(':');
        let secret = parts.next().expect("split should never give zero parts");
        let secret_with_prefix: [u8; 34] = multibase::decode(secret)
            .context("secret is not multibase encoded")?
            .1
            .try_into()
            .map_err(|_| {
                anyhow!("secret must be 0x8026 followed by 32 bytes of ed25519 private key")
            })?;
        let secret: [u8; 32] = secret_with_prefix
            .strip_prefix(ED25519_PRIVATE_KEY_MULTICODEC_PREFIX)
            .context("secret must be 0x8026 followed by 32 bytes of ed25519 private key")?
            .try_into()?;
        let key_pair = match parts.next() {
            None => Ed25519KeyPair::from_seed_unchecked(&secret)
                .map_err(|e| anyhow!("failed to create key pair from secret: {}", e))?,
            Some(public_multibase) => {
                let public_with_prefix: [u8; 34] = multibase::decode(public_multibase)
                    .context("public key is not multibase encoded")?
                    .1
                    .try_into()
                    .map_err(|_| {
                        anyhow!(
                            "public key must be 0xed01 followed by 32 bytes of ed25519 public key"
                        )
                    })?;
                let public: [u8; 32] = public_with_prefix
                    .strip_prefix(ED25519_PUBLIC_KEY_MULTICODEC_PREFIX)
                    .context(
                        "public key must be 0xed01 followed by 32 bytes of ed25519 public key",
                    )?
                    .try_into()?;

                Ed25519KeyPair::from_seed_and_public_key(&secret, &public).map_err(|e| {
                    anyhow!(
                        "failed to create key pair from secret and public key: {}",
                        e
                    )
                })?
            }
        };
        let public_ed25519_key_bytes = key_pair.public_key().as_ref().try_into()?;
        Ok((
            Self {
                public_ed25519_key_bytes,
            },
            key_pair,
        ))
    }
    /// public_ed25519_key_bytes from a PeerId
    pub fn try_from_peer_id(peer_id: &PeerId) -> Result<Self> {
        let peer_id_mh = peer_id.as_ref();
        if peer_id_mh.code() != 0x00 {
            return Err(anyhow!("peer ID multihash is not identity"));
        }
        if peer_id_mh.size() != 36 {
            return Err(anyhow!("peer ID multihash is not 36 bytes"));
        }
        let libp2p_key = peer_id_mh.digest();
        let ed25519_public_key = libp2p_key
            .strip_prefix(ED25519_LIBP2P_PEER_ID_PREFIX)
            .context(
                "libp2p peer ID must be 0x08011220 followed by 32 bytes of ed25519 public key",
            )?;
        let public_ed25519_key_bytes: [u8; 32] = ed25519_public_key.try_into()?;
        Ok(Self {
            public_ed25519_key_bytes,
        })
    }
    /// Create a NodeId using a random Ed25519 key pair
    pub fn random() -> (Self, String) {
        // Generate random secret key and corresponding keypair
        let random_secret = rand::thread_rng().gen::<[u8; 32]>();
        let key_pair = Ed25519KeyPair::from_seed_unchecked(random_secret.as_ref())
            .expect("expect 32 bytes to be well-formed");
        // Encode the public key and secret key
        let public_ed25519_key_bytes: [u8; 32] = key_pair
            .public_key()
            .as_ref()
            .try_into()
            .expect("expect public key to be 32 bytes");
        let public_key_with_prefix = [
            ED25519_PUBLIC_KEY_MULTICODEC_PREFIX,
            public_ed25519_key_bytes.as_ref(),
        ]
        .concat();
        let public_key_multibase =
            multibase::encode(multibase::Base::Base58Btc, public_key_with_prefix);
        let private_key_with_prefix = [
            ED25519_PRIVATE_KEY_MULTICODEC_PREFIX,
            random_secret.as_ref(),
        ]
        .concat();
        let private_key_multibase =
            multibase::encode(multibase::Base::Base58Btc, private_key_with_prefix);
        (
            Self {
                public_ed25519_key_bytes,
            },
            format!("{}:{}", private_key_multibase, public_key_multibase),
        )
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.did_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[test]
    fn test_ed25519_key_pair_from_secret() {
        let secret = "z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd";
        let (node_id_1, _) = NodeId::try_from_secret(secret).unwrap();
        let secret_and_public = "z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M";
        let (node_id_2, _) = NodeId::try_from_secret(secret_and_public).unwrap();
        assert_eq!(node_id_1, node_id_2);
        expect![["did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M"]]
            .assert_eq(&node_id_1.did_key());
    }

    #[test]
    fn test_did_from_peer_id() {
        let peer_id =
            PeerId::from_str("12D3KooWR1M8JiXyfdBKUhCLUmTJGhtNsgxnhvFVD4AU4EioDUwu").unwrap();
        let node_id = NodeId::try_from_peer_id(&peer_id).unwrap();
        expect![[r#"
            "did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M"
        "#]]
        .assert_debug_eq(&node_id.did_key());
    }

    #[test]
    fn test_peer_id_from_did() {
        let did = "did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M";
        let node_id = NodeId::try_from_did_key(did).unwrap();
        expect![[r#"
            PeerId(
                "12D3KooWR1M8JiXyfdBKUhCLUmTJGhtNsgxnhvFVD4AU4EioDUwu",
            )
        "#]]
        .assert_debug_eq(&node_id.peer_id());
    }
}
