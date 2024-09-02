use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use ceramic_core::Cid;
use ceramic_event::unvalidated::signed::cacao;
use ipld_core::ipld::Ipld;
use multihash_codetable::{Code, MultihashDigest as _};
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::cacao::Capability;

const DAG_CBOR: u64 = 0x71;

#[derive(Debug)]
pub struct WebAuthN {}

impl WebAuthN {
    /// Verify a cacao generated using WebAuthN (webauthn:p256)
    pub fn verify(cacao: &Capability) -> Result<()> {
        Self::verify_webauth_n(cacao)?;
        Ok(())
    }

    fn verify_webauth_n(cacao: &Capability) -> Result<()> {
        let meta = cacao
            .signature
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow!("missing signature metadata"))?;
        let AdditionalAuthenticatorData {
            auth_data,
            client_data_json,
        } = meta.try_as_ipld()?.try_into()?;

        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&cacao.signature.signature)
            .context("failed to decode signature")?;

        let b_iss = cacao
            .payload
            .issuer
            .split_once("did:key:z")
            .map_or(cacao.payload.issuer.as_str(), |(_, iss)| iss);

        let issuer_bytes = multibase::Base::Base58Btc
            .decode(b_iss)
            .context("failed to decode issuer")?;

        let (curve, public_key) = unsigned_varint::decode::u64(&issuer_bytes)
            .map_err(|e| anyhow!("failed to decode curve used: {}", e))?;
        if curve != 0x1200 {
            bail!("expected PublicKey to belong to curve p256");
        }

        let verifying_key = VerifyingKey::from_sec1_bytes(public_key)?;
        let sig = if sig.len() == 64 {
            Signature::from_slice(&sig).context("failed to generate signature")?
        } else {
            Signature::from_der(&sig).context("failed to generate signature from der")?
        };

        // Verify clientData authencity
        let client_data_hash = Code::Sha2_256.digest(client_data_json.as_slice());

        verifying_key.verify(
            [auth_data.as_slice(), client_data_hash.digest()]
                .concat()
                .as_slice(),
            &sig,
        )?;

        // Verify clientDataJSON.challenge equals message hash
        let data: ClientData = serde_json::from_slice(client_data_json.as_slice())?;
        if data.type_ != "webauthn.get" {
            bail!("invalid clientDataJSON.type")
        }
        let expected_hash = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&data.challenge)
            .context("failed to decode challenge")?;

        let challenge = Challenge::from(cacao);
        let block =
            serde_ipld_dagcbor::to_vec(&challenge).context("failed to cbor encode challenge")?;

        let digest = Code::Sha2_256.digest(block.as_slice());

        let cid = Cid::new_v1(DAG_CBOR, digest).to_bytes();
        // Compare reproduced challenge-hash to signed hash
        if expected_hash != cid {
            bail!("MessageMismatch")
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct AdditionalAuthenticatorData {
    auth_data: Vec<u8>,
    client_data_json: Vec<u8>,
}

// It appears this data is encoded as cbor bytes, and then encoded as an IPLD map, and then put into a DAG-CBOR object with the capability
// so I haven't figured out a nice way to use serde deserialization to get this to work yet. There might be something with deserialize_with
// or just implementing deserialize directly but for now, just keeping this here and only exposing IPLD on the CACAO.
impl TryFrom<&Ipld> for AdditionalAuthenticatorData {
    type Error = anyhow::Error;

    fn try_from(value: &Ipld) -> std::result::Result<Self, Self::Error> {
        let aad_bytes = if let ipld_core::ipld::Ipld::Map(map) = value {
            map.get("aad").ok_or_else(|| anyhow!("missing aad data"))?
        } else {
            bail!("aad must be a map")
        };

        let aad_data = if let ipld_core::ipld::Ipld::Bytes(aad_bytes) = aad_bytes {
            serde_ipld_dagcbor::from_slice::<Ipld>(aad_bytes)
                .context("failed to decode aad bytes as ipld")?
        } else {
            bail!("aad data must be bytes")
        };

        match aad_data {
            Ipld::Map(mut aad_map) => {
                let auth_data =
                    if let Some(ipld_core::ipld::Ipld::Bytes(b)) = aad_map.remove("authData") {
                        b
                    } else {
                        bail!("missing authData bytes");
                    };
                let client_data_json = if let Some(ipld_core::ipld::Ipld::Bytes(b)) =
                    aad_map.remove("clientDataJSON")
                {
                    b
                } else {
                    bail!("missing clientDataJSON bytes")
                };

                Ok(AdditionalAuthenticatorData {
                    auth_data,
                    client_data_json,
                })
            }

            _ => {
                bail!("decoded aad data must be a map")
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ClientData {
    #[serde(rename = "type")]
    type_: String,
    challenge: String,
}

#[derive(Clone, Debug, Serialize)]
struct Challenge<'a> {
    /// Header for capability
    #[serde(rename = "h")]
    header: &'a cacao::Header,
    /// Payload for capability
    #[serde(rename = "p")]
    payload: Payload<'a>,
}

/// Payload for a CACAO without certain fields using with webauthN challenges.
/// The public key is not known at pre-sign time so we sign a "challenge"-block without Issuer attribute
#[derive(Clone, Debug, Serialize)]
struct Payload<'a> {
    /// Audience for payload
    #[serde(rename = "aud")]
    audience: &'a str,

    /// Domain for payload
    domain: &'a str,

    /// Expiration time
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    expiration: Option<&'a cacao::CapabilityTime>,

    /// Issued at time.
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
    #[serde(rename = "iat")]
    issued_at: &'a cacao::CapabilityTime,

    /// Not before time.
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    not_before: Option<&'a cacao::CapabilityTime>,

    /// Nonce of payload
    nonce: &'a str,

    /// Resources
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<&'a [String]>,

    /// Version of payload
    version: &'a str,
}

impl<'a> From<&'a Capability> for Challenge<'a> {
    fn from(cacao: &'a Capability) -> Self {
        Challenge {
            header: &cacao.header,
            payload: Payload {
                audience: &cacao.payload.audience,
                domain: &cacao.payload.domain,
                expiration: cacao.payload.expiration.as_ref(),
                issued_at: &cacao.payload.issued_at,
                not_before: cacao.payload.not_before.as_ref(),
                nonce: &cacao.payload.nonce,
                resources: cacao.payload.resources.as_deref(),
                version: &cacao.payload.version,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::{
        test::{get_test_event, verify_event_cacao},
        VerifyCacaoOpts,
    };

    #[test(tokio::test)]
    async fn webauthn_pkh_valid_init() {
        verify_event_cacao(
            crate::test::SigningType::WebAuthN,
            crate::test::TestEventType::SignedInit,
            &VerifyCacaoOpts::default(),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn webauthn_pkh_valid_data() {
        verify_event_cacao(
            crate::test::SigningType::WebAuthN,
            crate::test::TestEventType::SignedData,
            &VerifyCacaoOpts::default(),
        )
        .await;
    }

    #[test]
    fn webauthn_pkh_deterministic_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::WebAuthN,
            crate::test::TestEventType::DeterministicInit,
        );
    }
}
