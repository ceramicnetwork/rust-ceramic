use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use ceramic_core::{Cid, Jwk, StreamId};
use ceramic_event::unvalidated::signed::cacao::{
    Capability, HeaderType, SignatureType, SortedMetadata,
};
use ssi::did_resolve::ResolutionInputMetadata;

use super::{
    jws::{verify_jws, VerifyJwsInput},
    opts::VerifyCacaoOpts,
};

use crate::signature::{pkh_ethereum::PkhEthereum, pkh_solana::PkhSolana};

#[async_trait::async_trait]
pub trait Verifier {
    /// Verify the signature of the CACAO and ensure it is valid.
    async fn verify_signature(&self, opts: &VerifyCacaoOpts) -> Result<()>;
    /// Verify the time checks for the CACAO using the `VerifyOpts`
    fn verify_time_checks(&self, opts: &VerifyCacaoOpts) -> Result<()>;
    /// Verify this CACAO grants access to the requested resources
    fn verify_access(
        &self,
        stream_id: &StreamId,
        payload_cid: Option<Cid>,
        model: Option<&StreamId>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for Capability {
    async fn verify_signature(&self, opts: &VerifyCacaoOpts) -> anyhow::Result<()> {
        // verify signed from js-did is not required as it won't deserialize without a signature
        // is that something that is ever expected?
        self.verify_time_checks(opts)?;

        match self.header.r#type {
            HeaderType::EIP4361 => PkhEthereum::verify(self),
            HeaderType::CAIP122 => match self.signature.r#type {
                SignatureType::EIP191 => PkhEthereum::verify(self),
                SignatureType::EIP1271 => bail!("EIP1271 signature validation is unimplemented"),
                SignatureType::SolanaED25519 => PkhSolana::verify(self),
                SignatureType::TezosED25519 => bail!("Tezos signature validation is unimplemented"),
                SignatureType::StacksSECP256K1 => {
                    bail!("Stacks signature validation is unimplemented")
                }
                SignatureType::WebAuthNP256 => {
                    bail!("WebAuthN signature validation is unimplemented")
                }
                SignatureType::JWS => {
                    let meta = if let Some(meta) = &self.signature.metadata {
                        meta
                    } else {
                        anyhow::bail!("no metadata found for jws");
                    };
                    let did = meta
                        .kid
                        .split_once("#")
                        .map_or(meta.kid.as_str(), |(k, _)| k);
                    // TODO: use time here?
                    let signer_did = Jwk::resolve_did(did, &ResolutionInputMetadata::default())
                        .await
                        .context("failed to resolve did")?;

                    let jwk = Jwk::new(&signer_did)
                        .await
                        .map_err(|e| anyhow::anyhow!("failed to generate jwk for did: {}", e))?;

                    let payload =
                        serde_json::to_vec(&self.payload).context("failed to serialize payload")?;
                    let header = serde_json::to_vec(&SortedMetadata::from(meta))
                        .context("failed to seralize metadata")?;
                    let sig = base64::prelude::BASE64_URL_SAFE_NO_PAD
                        .decode(self.signature.signature.as_bytes())
                        .map_err(|e| anyhow::anyhow!("invalid signature: {}", e))?;
                    verify_jws(VerifyJwsInput {
                        jwk: &jwk,
                        header: header.as_slice(),
                        payload: payload.as_slice(),
                        alg: self.signature.r#type.algorithm(),
                        signature: &sig,
                    })
                    .await
                }
            },
        }
    }

    fn verify_time_checks(&self, opts: &VerifyCacaoOpts) -> anyhow::Result<()> {
        let at_time = opts.at_time.unwrap_or_else(chrono::Utc::now);

        if self.payload.issued_at()? > at_time + opts.clock_skew
            || self
                .payload
                .not_before()?
                .map_or(false, |nb| nb > at_time + opts.clock_skew)
        {
            anyhow::bail!("CACAO is not valid yet")
        }
        if opts.check_exp {
            if let Some(exp) = self.payload.expiration()? {
                if exp + opts.revocation_phaseout_secs + opts.clock_skew < at_time {
                    anyhow::bail!("CACAO has expired")
                }
            }
        }

        Ok(())
    }

    fn verify_access(
        &self,
        stream_id: &StreamId,
        payload_cid: Option<Cid>,
        model: Option<&StreamId>,
    ) -> Result<()> {
        let resources = self
            .payload
            .resources
            .as_ref()
            .ok_or_else(|| anyhow!("capability is missing resources"))?;

        if resources.contains(&"ceramic://*".to_owned())
            || resources.contains(&format!("ceramic://{stream_id}"))
            || payload_cid.map_or(false, |payload_cid| {
                resources.contains(&format!("ceramic://{stream_id}?payload={payload_cid}"))
            })
            || model.map_or(false, |model| {
                resources.contains(&format!("ceramic://*?model=${model}"))
            })
        {
            Ok(())
        } else {
            bail!("capability does not have appropriate permissions to update this stream");
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use test_log::test;

    use super::*;

    const CACAO_STAR_RESOURCES: &str = r#"{"h":{"t":"eip4361"},"p":{"aud":"did:key:z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P","domain":"test","exp":"2024-06-19T20:04:42.464Z","iat":"2024-06-12T20:04:42.464Z","iss":"did:pkh:eip155:1:0x3794d4f077c08d925ff8ff820006b7353299b200","nonce":"wPiCOcpkll","resources":["ceramic://*"],"statement":"Give this application access to some of your data on Ceramic","version":"1"},"s":{"s":"0xb266999263446ddb9bf588825e9ac08b545e655f6077e8d8579a8d6639c1167c56f7dae7ac70f7faed8c141af9e124a7eb4f77423a572b36144ada8ef2206cda1c","t":"eip191"}}"#;
    const CACAO_STREAM_RESOURCES: &str = r#"{"h":{"t":"eip4361"},"p":{"aud":"did:key:z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P","domain":"test","exp":"2024-06-19T20:04:42.464Z","iat":"2024-06-12T20:04:42.464Z","iss":"did:pkh:eip155:1:0x3794d4f077c08d925ff8ff820006b7353299b200","nonce":"wPiCOcpkll","resources":["ceramic://k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw"],"statement":"Give this application access to some of your data on Ceramic","version":"1"},"s":{"s":"0xb266999263446ddb9bf588825e9ac08b545e655f6077e8d8579a8d6639c1167c56f7dae7ac70f7faed8c141af9e124a7eb4f77423a572b36144ada8ef2206cda1c","t":"eip191"}}"#;
    const CACAO_NO_RESOURCES: &str = r#"{"h":{"t":"eip4361"},"p":{"aud":"did:key:z6Mkr4a3Z3FFaJF8YqWnWnVHqd5eDjZN9bDST6wVZC1hJ81P","domain":"test","exp":"2024-06-19T20:04:42.464Z","iat":"2024-06-12T20:04:42.464Z","iss":"did:pkh:eip155:1:0x3794d4f077c08d925ff8ff820006b7353299b200","nonce":"wPiCOcpkll","resources":[],"statement":"Give this application access to some of your data on Ceramic","version":"1"},"s":{"s":"0xb266999263446ddb9bf588825e9ac08b545e655f6077e8d8579a8d6639c1167c56f7dae7ac70f7faed8c141af9e124a7eb4f77423a572b36144ada8ef2206cda1c","t":"eip191"}}"#;

    #[test]
    fn valid_cacao_star_resources() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg") // cspell:disable-line
                .unwrap();
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
                .unwrap();
        let cid =
            Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(); // cspell:disable-line
        let cacao = serde_json::from_str::<Capability>(CACAO_STAR_RESOURCES).unwrap();
        cacao
            .verify_access(&stream, Some(cid), Some(&model))
            .unwrap();
        cacao.verify_access(&model, None, None).unwrap(); // wrong stream okay with *
    }

    #[test]
    fn valid_cacao_stream_resources() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg") // cspell:disable-line
                .unwrap();
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
                .unwrap();
        let cid =
            Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(); // cspell:disable-line
        let cacao = serde_json::from_str::<Capability>(CACAO_STREAM_RESOURCES).unwrap();
        cacao
            .verify_access(&stream, Some(cid), Some(&model))
            .unwrap();
    }

    #[test]
    fn invalid_cacao_resources() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg") // cspell:disable-line
                .unwrap();
        let cid =
            Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(); // cspell:disable-line
        let cacao = serde_json::from_str::<Capability>(CACAO_STREAM_RESOURCES).unwrap();
        if cacao.verify_access(&model, Some(cid), Some(&model)).is_ok() {
            panic!("should not have had access to stream")
        }
    }

    #[test]
    fn cacao_without_resources() {
        let model =
            StreamId::from_str("kjzl6hvfrbw6c90uwoyz8j519gxma787qbsfjtrarkr1huq1g1s224k7hopvsyg") // cspell:disable-line
                .unwrap();
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
                .unwrap();
        let cid =
            Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(); // cspell:disable-line
        let cacao = serde_json::from_str::<Capability>(CACAO_NO_RESOURCES).unwrap();
        if cacao
            .verify_access(&stream, Some(cid), Some(&model))
            .is_ok()
        {
            panic!("should not have had access to stream")
        }
    }
}
