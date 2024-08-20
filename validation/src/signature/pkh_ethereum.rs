use std::str::FromStr;

use anyhow::{bail, Result};
use chrono::Utc;
use k256::ecdsa::RecoveryId;
use k256::ecdsa::{Signature, VerifyingKey};
use once_cell::sync::Lazy;
use sha3::Digest;
use ssi::caip10::BlockchainAccountId;
use ssi::keccak;

use crate::{cacao::Capability, siwx_message::SiwxMessage};

const ETH_CHAIN: &str = "Ethereum";

static LEGACY_CHAIN_ID_REORG_DATE: Lazy<chrono::DateTime<Utc>> = Lazy::new(|| {
    chrono::DateTime::from_timestamp_millis(1663632000000)
        .expect("2022-09-20 is a valid timestamp in milliseconds")
});

#[derive(Debug)]
pub struct PkhEthereum {}

impl PkhEthereum {
    /// Verify a cacao generated using SIWE (did:pkh:eip155 with eip4361 capability)
    pub fn verify(cacao: &Capability) -> anyhow::Result<()> {
        Self::verify_eip191_signature(cacao)
    }

    fn verify_eip191_signature(cacao: &Capability) -> anyhow::Result<()> {
        let issuer = BlockchainAccountId::from_str(&cacao.payload.issuer.replace("did:pkh:", ""))?
            .account_address
            .to_lowercase();
        let siwx = SiwxMessage::from_cacao(cacao)?;
        // assume the message doesn't use eip55 for the ethereum address
        let mut recovered =
            Self::verify_message(&siwx.as_message(ETH_CHAIN), &cacao.signature.signature)
                .unwrap_or_default();

        if recovered != issuer {
            // try using eip55 address
            if let Ok(addr) = keccak::eip55_checksum_addr(&issuer) {
                recovered = Self::verify_message(
                    &siwx.as_message_with_address(ETH_CHAIN, &addr),
                    &cacao.signature.signature,
                )
                .unwrap_or_default();
            }
        }

        if recovered != issuer && cacao.payload.issued_at()? < *LEGACY_CHAIN_ID_REORG_DATE {
            // might be an old CACAOv1 format
            recovered = Self::verify_message(
                &siwx.as_legacy_chain_id_message(ETH_CHAIN),
                &cacao.signature.signature,
            )
            .unwrap_or_default();
        }
        if recovered != issuer {
            anyhow::bail!("Signature does not belong to the issuer");
        }
        Ok(())
    }

    fn verify_message(msg: &str, signature: &str) -> Result<String> {
        // Create the prefixed message as per the Ethereum standard
        let digest = keccak::hash_personal_message(msg);
        let signature_bytes = hex::decode(signature.strip_prefix("0x").unwrap_or(signature))?;
        if signature_bytes.len() != 65 {
            bail!(
                "Ethereum signature should be 65 bytes not {}",
                signature_bytes.len()
            );
        }

        // eth offsets the 0-1 recovery ID by 27 and doesn't use 2 or 3
        let mut v = signature_bytes[64];
        if v == 0 || v == 1 {
            v += 27;
        } else if v == 2 || v == 3 {
            bail!("Recovery ID 2 and 3 are not supported in Ethereum signing")
        }

        let pub_key = VerifyingKey::recover_from_prehash(
            &digest,
            &Signature::from_slice(&signature_bytes[..64])?,
            RecoveryId::from_byte(v as u8 - 27)
                .ok_or_else(|| anyhow::anyhow!("Invalid recovery ID"))?,
        )?;

        // derive Ethereum address (last 20 bytes of 32 byte hash skipping the format byte)
        let pub_key = pub_key.to_encoded_point(false);
        let hash = sha3::Keccak256::digest(&pub_key.as_bytes()[1..]);
        Ok(format!("0x{}", hex::encode(&hash[12..])))
    }
}

#[cfg(test)]
mod test {

    use ceramic_event::unvalidated;
    use ipld_core::ipld::Ipld;
    use test_log::test;

    use crate::{
        cacao_verifier::Verifier,
        test::{get_test_event, verify_event_cacao, CACAO_SIGNED_DATA_EVENT_CAR},
        VerifyOpts,
    };

    #[test(tokio::test)]
    async fn eth_pkh_valid_init() {
        verify_event_cacao(
            crate::test::SigningType::Ethereum,
            crate::test::TestEventType::SignedInit,
            &VerifyOpts::default(),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn eth_pkh_valid_data() {
        verify_event_cacao(
            crate::test::SigningType::Ethereum,
            crate::test::TestEventType::SignedData,
            &VerifyOpts::default(),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn eth_pkh_deterministic_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::Ethereum,
            crate::test::TestEventType::DeterministicInit,
        );
    }

    #[test(tokio::test)]
    async fn eth_pkh_valid_data_expired_time_checks() {
        let (_base, data) = multibase::decode(CACAO_SIGNED_DATA_EVENT_CAR).unwrap();
        let (_, event) = unvalidated::Event::<Ipld>::decode_car(data.as_slice(), false).unwrap();
        let expired_message = "CACAO has expired";
        match event {
            unvalidated::Event::Time(_) => unreachable!("not a time event"),
            unvalidated::Event::Signed(s) => {
                let cap = s.capability().unwrap();

                // valid without expired check
                match cap
                    .verify_signature(&VerifyOpts {
                        check_exp: false,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("{:#}", e)
                    }
                }

                // invalid with expired check
                match cap
                    .verify_signature(&VerifyOpts {
                        check_exp: true,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {
                        panic!("Should have been expired")
                    }
                    Err(e) => {
                        assert!(e.to_string().contains(expired_message))
                    }
                }

                // "iat": "2024-06-12T20:04:42.464Z"
                // "exp": "2024-06-19T20:04:42.464Z"
                // valid with at_time check
                match cap
                    .verify_signature(&VerifyOpts {
                        at_time: Some(
                            chrono::DateTime::parse_from_rfc3339("2024-06-15T20:04:42.464Z")
                                .unwrap()
                                .to_utc(),
                        ),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("should not have been expired using at_time: {}", e);
                    }
                }
                // valid 1 min before iat with at_time and clock skew check
                match cap
                    .verify_signature(&VerifyOpts {
                        at_time: Some(
                            chrono::DateTime::parse_from_rfc3339("2024-06-12T20:03:42.464Z")
                                .unwrap()
                                .to_utc(),
                        ),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!(
                            "should not have been expired using at_time before iat + skew: {}",
                            e
                        );
                    }
                }

                // valid 1 min after exp with at_time and clock skew check
                match cap
                    .verify_signature(&VerifyOpts {
                        at_time: Some(
                            chrono::DateTime::parse_from_rfc3339("2024-06-19T20:05:42.464Z")
                                .unwrap()
                                .to_utc(),
                        ),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!(
                            "should not have been expired using at_time after exp + skew: {}",
                            e
                        );
                    }
                }

                // invalid 10 min after exp with at_time w/o recovation seconds
                match cap
                    .verify_signature(&VerifyOpts {
                        at_time: Some(
                            chrono::DateTime::parse_from_rfc3339("2024-06-19T20:14:42.464Z")
                                .unwrap()
                                .to_utc(),
                        ),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {
                        panic!(
                            "should have been expired using at_time after exp + skew w/o recovation",
                        );
                    }
                    Err(e) => {
                        assert!(e.to_string().contains(expired_message))
                    }
                }

                // valid 10 min after exp with at_time with recovation seconds
                match cap
                    .verify_signature(&VerifyOpts {
                        at_time: Some(
                            chrono::DateTime::parse_from_rfc3339("2024-06-19T20:14:42.464Z")
                                .unwrap()
                                .to_utc(),
                        ),
                        revocation_phaseout_secs: chrono::TimeDelta::from_std(
                            std::time::Duration::from_secs(60 * 5),
                        )
                        .unwrap(),

                        ..Default::default()
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!(
                            "should not have been expired using at_time after exp + skew + revocation: {}",
                            e
                        );
                    }
                }
            }
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }
}
