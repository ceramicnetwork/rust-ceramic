use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed::{self, cacao::Capability};
use ipld_core::ipld::Ipld;
use ssi::{did_resolve::ResolutionInputMetadata, jws::Header};

use super::{
    cacao_verifier::Verifier as _,
    jws::{jws_digest, verify_jws, VerifyJwsInput},
    opts::{AtTime, VerifyJwsOpts},
};

#[async_trait::async_trait]
/// Trait to represeng verifying a signature.
pub trait Verifier {
    /// Verify the JWS signature against the controller (issuer) if provided.
    /// Without issuer simply verifies the JWS and CACAO (if included) are valid, without following
    /// the DID delegation chain for the controller.
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()>;
}

/// Struct to wrap pieces needed from the Ceramic event being validated that makes testing easier.
/// We don't have to build events from the carfile, but can build the parts from json and construct this.
pub(crate) struct SignatureData<'a> {
    jws_header: Header,
    /// The event signature bytes
    signature: &'a [u8],
    /// The protected header bytes (the i.e. the `jws_header` field before being deserialized).
    /// We could compute this by serializing the data but the caller already has it.
    /// Using `ceramic_event::unvalidated::signed::Signature` would be nicer but is harder for testing
    /// as we can't easily construct one from the bytes since things are private, and getting the
    /// deserialization right is currently eluding me without reading the dag-jose bytes directly.
    protected: &'a [u8],
    /// The payload being signed over
    payload: &'a [u8],
    /// The CACAO used to delegate signing permission
    capability: Option<&'a Capability>,
}

impl<'a> SignatureData<'a> {
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()> {
        let did = self
            .jws_header
            .key_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing jws kid"))?;

        // Should add time checks for revoked DID key and if it was signed in the valid window
        let signer_did = Jwk::resolve_did(did, &ResolutionInputMetadata::default())
            .await
            .context("failed to resolve did")?;

        let jwk = Jwk::new(&signer_did)
            .await
            .map_err(|e| anyhow!("failed to generate jwk for did: {}", e))?;

        let signer_did = &signer_did.id;

        if let Some(cacao) = self.capability {
            if cacao.payload.audience != *signer_did {
                bail!("signer '{signer_did}' was not granted permission by capability")
            }
            if let Some(issuer) = controller {
                if issuer_equals(issuer, &cacao.payload.issuer) {
                    // if controller is the cacao issuer being valid is sufficient
                } else if issuer != signer_did {
                    // we have to resolve the controller DID, check its controller list
                    // and make sure the issuer is included
                    resolve_did_verify_delegated(issuer, &cacao.payload.issuer, &opts.at_time)
                        .await?;
                } else {
                    // if the issuer (controller) is the signer, a valid CACAO is sufficient since
                    // we delegated to them as audience
                }
            }

            cacao.verify_signature(&opts.to_owned().into()).await?;
        } else if let Some(issuer) = controller {
            if issuer != signer_did {
                // if it's not a CACAO and the signer isn't the controller, we
                // make sure the signer delgated through the DID controllers list
                resolve_did_verify_delegated(issuer, signer_did, &opts.at_time).await?;
            }
        }

        verify_jws(VerifyJwsInput {
            jwk: &jwk,
            jws_digest: &jws_digest(self.protected, self.payload),
            alg: self.jws_header.algorithm,
            signature: self.signature,
        })
        .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Verifier for signed::Event<Ipld> {
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()> {
        let signature = self
            .envelope()
            .signature()
            .first()
            .ok_or_else(|| anyhow!("missing signature on signed event"))?;

        let protected = signature
            .protected()
            .ok_or_else(|| anyhow!("missing protected field"))?
            .as_slice();

        let jws_header = signature.jws_header().context("signature is not jws")?;
        let payload = self.envelope().payload().as_slice();
        let signature = signature.signature().as_slice();
        let to_verify = SignatureData {
            jws_header,
            protected,
            payload,
            signature,
            capability: self.capability().map(|(_, ref c)| c),
        };

        to_verify.verify_signature(controller, opts).await
    }
}

/// Resolve the controller DID and verify the delegated DID is in the controller list
async fn resolve_did_verify_delegated(issuer: &str, delegated: &str, time: &AtTime) -> Result<()> {
    let mut meta = ResolutionInputMetadata::default();
    let controller_did = match time {
        AtTime::At(t) => {
            meta.version_time = Some(
                t.unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            );
            Jwk::resolve_did(issuer, &meta)
                .await
                .context(format!("failed to resolve issuer did with time: {issuer}"))?
        }
        AtTime::SkipTimeChecks => Jwk::resolve_did(issuer, &meta)
            .await
            .context(format!("failed to resolve issuer did: {issuer}"))?,
    };
    if controller_did
        .controller
        .as_ref()
        .map_or(true, |c| !c.any(|c| c == delegated))
    {
        bail!("invalid_jws: '{delegated}' not in controllers list for issuer: '{issuer}'")
    }
    Ok(())
}

fn issuer_equals(did_a: &str, did_b: &str) -> bool {
    if did_a == did_b {
        true
    } else if did_a.starts_with("did:pkh:eip155:1:") {
        did_a.to_lowercase() == did_b.to_lowercase()
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use ceramic_event::unvalidated;
    use test_log::test;

    use super::*;

    use crate::{
        test::{
            assert_invalid_event, verify_event, SigningType, TestEventType, SIGNED_DATA_EVENT_CAR,
            SIGNED_INIT_EVENT_CAR,
        },
        verifier::opts::VerifyJwsOpts,
    };

    const TEST_DID: &str = "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9";
    const TEST_DID2: &str = "did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw";

    #[test(tokio::test)]
    async fn resolve_dids_not_delegated() {
        for did in [TEST_DID, TEST_DID2] {
            match resolve_did_verify_delegated(did, did, &AtTime::SkipTimeChecks).await {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
            match resolve_did_verify_delegated(did, did, &AtTime::At(Some(chrono::Utc::now())))
                .await
            {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
            match resolve_did_verify_delegated(did, did, &AtTime::At(None)).await {
                Ok(_) => panic!("should have errored: {did}"),
                Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{did} {:#}", e),
            }
        }
    }

    #[test(tokio::test)]
    async fn valid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            verify_event(e_type, TestEventType::SignedInit, &VerifyJwsOpts::default()).await;
        }
    }

    #[test(tokio::test)]
    async fn valid_data_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            verify_event(e_type, TestEventType::SignedData, &VerifyJwsOpts::default()).await;
        }
    }

    #[test(tokio::test)]
    async fn invalid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            assert_invalid_event(
                e_type,
                TestEventType::InvalidSignedInit,
                &VerifyJwsOpts::default(),
            )
            .await;
        }
    }

    #[test(tokio::test)]
    async fn invalid_data_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
            SigningType::WebAuthN,
        ] {
            assert_invalid_event(
                e_type,
                TestEventType::InvalidSignedData,
                &VerifyJwsOpts::default(),
            )
            .await;
        }
    }

    #[test(tokio::test)]
    async fn local_car_init_event() {
        let (_, bytes) = multibase::decode(
            SIGNED_INIT_EVENT_CAR
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .unwrap();
        let (_cid, event) =
            unvalidated::Event::<Ipld>::decode_car(std::io::Cursor::new(bytes), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!("not time"),
            unvalidated::Event::Signed(s) => s
                .verify_signature(
                    None,
                    &VerifyJwsOpts {
                        at_time: AtTime::SkipTimeChecks,
                        ..Default::default()
                    },
                )
                .await
                .expect("should be valid"),

            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test(tokio::test)]
    async fn local_car_data_event() {
        let (_, bytes) = multibase::decode(
            SIGNED_DATA_EVENT_CAR
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>(),
        )
        .unwrap();
        let (_cid, event) =
            unvalidated::Event::<Ipld>::decode_car(std::io::Cursor::new(bytes), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!("not time"),
            unvalidated::Event::Signed(s) => s
                .verify_signature(
                    None,
                    &VerifyJwsOpts {
                        at_time: AtTime::SkipTimeChecks,
                        ..Default::default()
                    },
                )
                .await
                .expect("should be valid"),

            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }
}
