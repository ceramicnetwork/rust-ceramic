use std::str::FromStr as _;

use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey, PUBLIC_KEY_LENGTH};
use ssi::caip10::BlockchainAccountId;

use crate::{cacao::Capability, siwx_message::SiwxMessage};

const SOLANA_CHAIN: &str = "Solana";

#[derive(Debug)]
pub struct PkhSolana {}

impl PkhSolana {
    /// Verify a cacao generated using SIWSolana (did:pkh:solana)
    pub fn verify(cacao: &Capability) -> Result<()> {
        Self::verify_solana_signature(cacao)?;
        Ok(())
    }

    fn verify_solana_signature(cacao: &Capability) -> Result<()> {
        let sig = multibase::Base::Base58Btc
            .decode(&cacao.signature.signature)
            .context("failed to decode signature")?;

        let issuer =
            BlockchainAccountId::from_str(cacao.payload.issuer.replace("did:pkh:", "").as_str())?
                .account_address;
        let issuer_bytes = multibase::Base::Base58Btc
            .decode(&issuer)
            .context("failed to decode issuer")?;

        let issuer_bytes: &[u8; PUBLIC_KEY_LENGTH] = &issuer_bytes[0..PUBLIC_KEY_LENGTH]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid issuer length"))?;
        let pub_key =
            VerifyingKey::from_bytes(issuer_bytes).context("failed to recover public key")?;

        let siwx = SiwxMessage::from_cacao(cacao)?;
        let msg = siwx.as_message(SOLANA_CHAIN);

        let sig = Signature::from_slice(&sig).context("failed to generate signature")?;
        pub_key.verify(msg.as_bytes(), &sig)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::{
        test::{get_test_event, verify_event_cacao},
        VerifyOpts,
    };

    #[test(tokio::test)]
    async fn sol_pkh_valid_init() {
        verify_event_cacao(
            crate::test::SigningType::Solana,
            crate::test::TestEventType::SignedInit,
            &VerifyOpts::default(),
        )
        .await;
    }

    #[test(tokio::test)]
    async fn sol_pkh_valid_data() {
        verify_event_cacao(
            crate::test::SigningType::Solana,
            crate::test::TestEventType::SignedData,
            &VerifyOpts::default(),
        )
        .await;
    }

    #[test]
    fn sol_pkh_deterministic_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::Solana,
            crate::test::TestEventType::DeterministicInit,
        );
    }
}
