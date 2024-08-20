use anyhow::Result;
use ceramic_event::unvalidated::signed;
use ipld_core::ipld::Ipld;

use super::{cacao_verifier::Verifier as _, key_verifier::Verifier as _, opts::VerifyOpts};

#[async_trait::async_trait]
pub trait Verifier {
    async fn verify_signature(&self, opts: &VerifyOpts) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for signed::Event<Ipld> {
    async fn verify_signature(&self, opts: &VerifyOpts) -> Result<()> {
        let envelope = self.envelope();
        let delegated_kid = if let Some(cacao) = self.capability() {
            cacao.verify_signature(opts).await?;
            Some(cacao.payload.audience.as_str())
        } else {
            None
        };
        // verify envelope signature ensuring cacao aud matches DID for cacao
        envelope.verify_signature(delegated_kid).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::{
        test::{verify_event, SigningType, TestEventType},
        VerifyOpts,
    };

    #[test(tokio::test)]
    async fn valid_init_signatures() {
        for e_type in [
            SigningType::Solana,
            SigningType::Ethereum,
            SigningType::Ed2559,
            SigningType::EcdsaP256,
        ] {
            verify_event(e_type, TestEventType::SignedInit, &VerifyOpts::default()).await;
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
            verify_event(e_type, TestEventType::SignedData, &VerifyOpts::default()).await;
        }
    }
}
