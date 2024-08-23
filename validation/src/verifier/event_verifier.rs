use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed;
use ipld_core::ipld::Ipld;
use ssi::did_resolve::ResolutionInputMetadata;

use super::{
    cacao_verifier::Verifier as _,
    jws::{verify_jws, VerifyJwsInput},
    opts::{AtTime, VerifyJwsOpts},
};

#[async_trait::async_trait]
pub trait Verifier {
    /// Verify the JWS signature against the controller (issuer) if provided.
    /// Without issuer simply verifies the JWS and CACAO (if included) are valid, without following
    /// the DID delegation chain for the controller.
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for signed::Event<Ipld> {
    async fn verify_signature(&self, controller: Option<&str>, opts: &VerifyJwsOpts) -> Result<()> {
        let signature = self
            .envelope()
            .signature()
            .first()
            .ok_or_else(|| anyhow!("missing signature on signed event"))?;

        let jws_header = self
            .envelope()
            .jws_header()
            .context("event envelope is not a jws")?;

        let protected = signature
            .protected
            .as_ref()
            .ok_or_else(|| anyhow!("missing protected field"))?;

        let did = &jws_header
            .key_id
            .ok_or_else(|| anyhow!("missing jws kid"))?;

        let signer_did = Jwk::resolve_did(did, &ResolutionInputMetadata::default())
            .await
            .context("failed to resolve did")?;

        let jwk = Jwk::new(&signer_did)
            .await
            .map_err(|e| anyhow!("failed to generate jwk for did: {}", e))?;

        let signer_did = &signer_did.id;

        if let Some(cacao) = self.capability() {
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
                    // if the controller is the signer, a valid CACAO is sufficient since
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
            header: protected.as_slice(),
            payload: self.envelope().payload().as_slice(),
            alg: jws_header.algorithm,
            signature: signature.signature.as_slice(),
        })
        .await?;

        Ok(())
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
                .context("failed to resolve issuer did with time")?
        }
        AtTime::SkipTimeChecks => Jwk::resolve_did(issuer, &meta)
            .await
            .context("failed to resolve issuer did")?,
    };
    if controller_did
        .controller
        .as_ref()
        .map_or(true, |c| !c.any(|c| c == delegated))
    {
        bail!("invalid_jws: '{delegated}' not in controllers list for issuer: {issuer}")
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

    #[test(tokio::test)]
    async fn resolve_dids_not_delegated() {
        match resolve_did_verify_delegated(TEST_DID, TEST_DID, &AtTime::SkipTimeChecks).await {
            Ok(_) => panic!("should have errored"),
            Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{}", e),
        }
        match resolve_did_verify_delegated(
            TEST_DID,
            TEST_DID,
            &AtTime::At(Some(chrono::Utc::now())),
        )
        .await
        {
            Ok(_) => panic!("should have errored"),
            Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{}", e),
        }
        match resolve_did_verify_delegated(TEST_DID, TEST_DID, &AtTime::At(None)).await {
            Ok(_) => panic!("should have errored"),
            Err(e) => assert!(e.to_string().starts_with("invalid_jws:"), "{}", e),
        }
    }

    #[test(tokio::test)]
    async fn valid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
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
