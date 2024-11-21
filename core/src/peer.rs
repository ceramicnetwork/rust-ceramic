use anyhow::{anyhow, bail};
use multiaddr::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use ssi::jws::DecodedJWS;

use crate::{node_id::NodeKey, signer::Signer, DeserializeExt as _, NodeId, SerializeExt as _};

/// Peer entry that is signed and can be shared.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerEntry {
    id: NodeId,
    // Number of seconds after UNIX epoch when this entry is no longer valid.
    expiration: u32,
    addresses: Vec<Multiaddr>,
}

impl PeerEntry {
    /// Construct an entry about a peer with address that is no longer valid after expiration seconds after the
    /// UNIX epoch.
    pub fn new(local_id: NodeId, expiration: u32, addresses: Vec<Multiaddr>) -> Self {
        let peer_id = local_id.peer_id();
        Self {
            id: local_id,
            expiration,
            addresses: addresses
                .into_iter()
                .map(|addr| ensure_multiaddr_has_p2p(addr, peer_id))
                .collect(),
        }
    }

    fn from_jws(jws: &str) -> anyhow::Result<Self> {
        let (header_b64, payload_enc, signature_b64) = ssi::jws::split_jws(jws)?;
        let DecodedJWS {
            header,
            signing_input,
            payload,
            signature,
        } = ssi::jws::decode_jws_parts(header_b64, payload_enc.as_bytes(), signature_b64)?;
        let mut entry = PeerEntry::from_json(&payload)?;
        let peer_id = entry.id.peer_id();
        entry.addresses = entry
            .addresses
            .into_iter()
            .map(|addr| ensure_multiaddr_has_p2p(addr, peer_id))
            .collect();
        let key = entry.id.jwk();
        ssi::jws::verify_bytes(header.algorithm, &signing_input, &key, &signature)?;
        Ok(entry)
    }
    fn to_jws(&self, signer: impl Signer) -> anyhow::Result<String> {
        let entry = self.to_json()?;
        signer.sign_jws(&entry)
    }

    /// Report the id of this peer.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Report the number of seconds after the UNIX epoch when this entry is no longer valid.
    pub fn expiration(&self) -> u32 {
        self.expiration
    }

    /// Report the addresses where this peer can be dialed. These are guaranteed to contain the
    /// peer id within the address.
    pub fn addresses(&self) -> &[Multiaddr] {
        &self.addresses
    }
}

fn ensure_multiaddr_has_p2p(addr: Multiaddr, peer_id: PeerId) -> Multiaddr {
    if !addr.iter().any(|protocol| match protocol {
        multiaddr::Protocol::P2p(id) => id == peer_id,
        _ => false,
    }) {
        addr.with(multiaddr::Protocol::P2p(peer_id))
    } else {
        addr
    }
}

/// Encoded [`PeerEntry`] prefixed with its expiration.
/// The sort order matters as its used in a Recon ring.
/// The key is valid utf-8 of the form `<expiration>.<jws>`;
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerKey(String);

impl PeerKey {
    /// Construct a signed key from a [`PeerEntry`].
    pub fn from_entry(entry: &PeerEntry, node_key: &NodeKey) -> anyhow::Result<Self> {
        if entry.id() != node_key.id() {
            bail!("peer key must be signed by its own ID")
        }
        Ok(Self(format!(
            // 11 digits of a timestamp gets us 1000+ years of padding for a consistent sort order.
            "{:0>11}.{}",
            entry.expiration,
            entry.to_jws(node_key)?
        )))
    }
    /// Decode and verify key as a [`PeerEntry`].
    pub fn to_entry(&self) -> anyhow::Result<PeerEntry> {
        let idx = self
            .0
            .find('.')
            .ok_or_else(|| anyhow!("peer key must contain a '.'"))?;
        let expiration: u32 = self.0[0..idx].parse()?;
        let jws = &self.0[idx + 1..];
        let peer = PeerEntry::from_jws(jws)?;
        if expiration != peer.expiration {
            Err(anyhow!(
                "peer key expiration must match peer entry: {expiration} != {}",
                peer.expiration
            ))
        } else {
            Ok(peer)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{PeerEntry, PeerKey};

    use anyhow::Result;
    use expect_test::expect;
    use test_log::test;
    use tracing::debug;

    use crate::node_id::NodeKey;

    #[test]
    fn peer_roundtrip() -> Result<()> {
        let node_key = NodeKey::random();
        let entry = PeerEntry::new(
            node_key.id(),
            1732211100,
            ["/ip4/127.0.0.1/tcp/5100", "/ip4/127.0.0.2/udp/5100/quic-v1"]
                .into_iter()
                .map(std::str::FromStr::from_str)
                .collect::<Result<_, _>>()?,
        );
        debug!(?entry, "peer entry");
        let key = PeerKey::from_entry(&entry, &node_key)?;
        debug!(?key, "peer key");
        assert_eq!(entry, key.to_entry()?);
        Ok(())
    }
    #[test]
    fn peer_entry_p2p_multiaddrs() -> Result<()> {
        let node_key =
            NodeKey::try_from_secret("z3u2WLX8jeyN6sfbDowLGudoZHudxgVkNJfrw2TDTVx4tijd")?;
        let entry = PeerEntry::new(
            node_key.id(),
            1732211100,
            ["/ip4/127.0.0.1/tcp/5100", "/ip4/127.0.0.2/udp/5100/quic-v1"]
                .into_iter()
                .map(std::str::FromStr::from_str)
                .collect::<Result<_, _>>()?,
        );
        expect![[r#"
            PeerEntry {
                id: did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M,
                expiration: 1732211100,
                addresses: [
                    /ip4/127.0.0.1/tcp/5100/p2p/12D3KooWR1M8JiXyfdBKUhCLUmTJGhtNsgxnhvFVD4AU4EioDUwu,
                    /ip4/127.0.0.2/udp/5100/quic-v1/p2p/12D3KooWR1M8JiXyfdBKUhCLUmTJGhtNsgxnhvFVD4AU4EioDUwu,
                ],
            }
        "#]]
        .assert_debug_eq(&entry);
        let key = PeerKey::from_entry(&entry, &node_key)?;
        expect![[r#"
            PeerKey(
                "01732211100.eyJhbGciOiJFZERTQSIsImtpZCI6Ino2TWt1ZUYxOXFDaHBHUUpCSlhjWGpmb00xTVlDd0MxNjdSTXdVaU5XWFh2RW0xTSJ9.eyJpZCI6eyJwdWJsaWNfZWQyNTUxOV9rZXlfYnl0ZXMiOlsyMjUsMTc1LDEzMSwxODYsNDIsNTIsMTg2LDEyMiw0OCwxMzEsOTIsNTIsMTI3LDE4MywyMjYsMTcsMiw2MCwxMDgsMTY2LDEwMCw0NCwyMTksMzIsMTgsMjMwLDI0Miw2NywxNTQsMTg0LDE1NCw5Ml19LCJleHBpcmF0aW9uIjoxNzMyMjExMTAwLCJhZGRyZXNzZXMiOlsiL2lwNC8xMjcuMC4wLjEvdGNwLzUxMDAvcDJwLzEyRDNLb29XUjFNOEppWHlmZEJLVWhDTFVtVEpHaHROc2d4bmh2RlZENEFVNEVpb0RVd3UiLCIvaXA0LzEyNy4wLjAuMi91ZHAvNTEwMC9xdWljLXYxL3AycC8xMkQzS29vV1IxTThKaVh5ZmRCS1VoQ0xVbVRKR2h0TnNneG5odkZWRDRBVTRFaW9EVXd1Il19.X1LOJlSQSMAMyYhO8OhpjKJ-Q2SqoTuw6Ak-O6ZZN6oEl1XNLsuf2smq5CotYZPTKhRqPazwBEZzm5K3SEz1Cw",
            )
        "#]].assert_debug_eq(&key);
        Ok(())
    }
    #[test]
    fn peer_jws_verify() -> Result<()> {
        let n1 = NodeKey::random();
        let n2 = NodeKey::random();
        let entry = PeerEntry::new(
            n1.id(),
            1732211100,
            ["/ip4/127.0.0.1/tcp/5100", "/ip4/127.0.0.2/udp/5100/quic-v1"]
                .into_iter()
                .map(std::str::FromStr::from_str)
                .collect::<Result<_, _>>()?,
        );
        let jws = entry.to_jws(&n2)?;
        expect![[r#"
            Err(
                JWK(
                    CryptoErr(
                        signature::Error { source: Some(Verification equation was not satisfied) },
                    ),
                ),
            )
        "#]]
        .assert_debug_eq(&PeerEntry::from_jws(&jws));
        Ok(())
    }
}
