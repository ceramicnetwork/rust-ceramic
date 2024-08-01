use anyhow::Result;
use ring::signature::{Ed25519KeyPair, KeyPair};

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
    let public_multibase = multibase::encode(multibase::Base::Base58Btc, &public_with_prefix);
    format!("did:key:{}", public_multibase)
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
        assert!(did_key_from_ed25519_key_pair(&key1) == did_key_from_ed25519_key_pair(&key2));
        assert!(
            did_key_from_ed25519_key_pair(&key1)
                == "did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M"
        );
        println!("{}", did_key_from_ed25519_key_pair(&key1));
    }
}
