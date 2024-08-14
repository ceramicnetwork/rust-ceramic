use std::{fs, path::PathBuf, str::FromStr};

use anyhow::{anyhow, Result};
use cid::multihash::Multihash;
use cid::Cid;
use ring::signature::{Ed25519KeyPair, KeyPair};

/// Read an Ed25519 key from a directory and return a key pair
pub async fn read_ed25519_key_from_dir(p2p_key_dir: PathBuf) -> Result<Ed25519KeyPair> {
    let key_path = p2p_key_dir.join("id_ed25519_0");
    let content = fs::read_to_string(&key_path)?;
    let seed = ssh_key::private::PrivateKey::from_str(&content)
        .map_err(|e| anyhow::anyhow!("failed to parse private key: {}", e))?
        .key_data()
        .ed25519()
        .map_or(Err(anyhow::anyhow!("failed to parse ed25519 key")), |key| {
            Ok(key.private.to_bytes())
        })?;
    Ed25519KeyPair::from_seed_unchecked(seed.as_ref())
        .map_err(|e| anyhow::anyhow!("failed to create key pair: {}", e))
}

/// Create an Ed25519 key pair from a secret
pub fn ed25519_key_pair_from_secret(secret: &str) -> Result<Ed25519KeyPair> {
    let mut parts = secret.split(':');
    let secret_with_prefix: [u8; 34] = multibase::decode(parts.next().unwrap())?
        .1
        .try_into()
        .unwrap();
    let secret: [u8; 32] = secret_with_prefix
        .strip_prefix(b"\x80\x26")
        .unwrap()
        .try_into()?;
    match parts.next() {
        None => Ok(Ed25519KeyPair::from_seed_unchecked(&secret).unwrap()),
        Some(public_multibase) => {
            let public_with_prefix: [u8; 34] =
                multibase::decode(public_multibase)?.1.try_into().unwrap();
            let public: [u8; 32] = public_with_prefix
                .strip_prefix(b"\xed\x01")
                .unwrap()
                .try_into()?;
            Ok(Ed25519KeyPair::from_seed_and_public_key(&secret, &public).unwrap())
        }
    }
}

/// Create a DID key from an Ed25519 key pair
pub fn did_key_from_ed25519_key_pair(key: &Ed25519KeyPair) -> String {
    let public = key.public_key().as_ref();
    let public_with_prefix = [b"\xed\x01", public].concat();
    let public_multibase = multibase::encode(multibase::Base::Base58Btc, public_with_prefix);
    format!("did:key:{}", public_multibase)
}

/// Create a CID from an Ed25519 key pair
pub fn cid_from_ed25519_key_pair(key: &Ed25519KeyPair) -> Cid {
    let public = key.public_key().as_ref();
    let hash = Multihash::<64>::wrap(0, public).expect("ed25519 public key is 32 bytes");
    Cid::new_v1(0xed, hash)
}

/// Create a DID from a Libp2p Peer ID Multihash
pub fn did_from_peer_id(peer_id_mh: &Multihash<64>) -> Result<String> {
    if peer_id_mh.code() != 0x00 {
        return Err(anyhow!("peer ID multihash is not identity"));
    }
    if peer_id_mh.size() != 36 {
        return Err(anyhow!("peer ID multihash is not 36 bytes"));
    }
    let libp2p_key = peer_id_mh.digest();
    let ed25519_public_key = libp2p_key.strip_prefix(b"\x08\x01\x12\x20").unwrap();
    let ed25519_public_key_with_prefix = [b"\xed\x01", ed25519_public_key].concat();
    let ed25519_public_key_multibase =
        multibase::encode(multibase::Base::Base58Btc, ed25519_public_key_with_prefix);
    Ok(format!("did:key:{}", ed25519_public_key_multibase))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_key_pair_from_secret() {
        let secret = "z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd";
        let key1 = ed25519_key_pair_from_secret(secret).unwrap();
        let secret_and_public = "z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M";
        let key2 = ed25519_key_pair_from_secret(secret_and_public).unwrap();
        assert_eq!(
            did_key_from_ed25519_key_pair(&key1),
            did_key_from_ed25519_key_pair(&key2)
        );
        assert_eq!(
            did_key_from_ed25519_key_pair(&key1),
            "did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M"
        );
        println!("{}", did_key_from_ed25519_key_pair(&key1));
    }
}
