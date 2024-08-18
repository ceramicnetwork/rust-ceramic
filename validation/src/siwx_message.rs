use std::str::FromStr;

use anyhow::{Context, Result};
use ssi::{caip10::BlockchainAccountId, caip2::ChainId};

// use ssi_caips::{caip10::BlockchainAccountId, caip2::ChainId};
use crate::cacao::Capability;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SiwxMessage<'a> {
    ///RFC 4501 dns authority that is requesting the signing.
    domain: &'a str,
    /// Address performing the signing conformant to capitalization
    /// encoded checksum specified in EIP-55 where applicable if Ethereum.
    address: String,
    /// Human-readable ASCII assertion that the user will sign, and it must not
    /// contain `\n`.
    statement: Option<String>,
    /// RFC 3986 URI referring to the resource that is the subject of the signing
    /// (as in the __subject__ of a claim).
    uri: &'a str,
    /// Current version of the message.
    version: &'a str,
    /// Randomized token used to prevent replay attacks, at least 8 alphanumeric
    /// characters.
    nonce: &'a str,
    /// ISO 8601 datetime String of the current time.
    issued_at: String,
    /// ISO 8601 datetime String that, if present, indicates when the signed
    /// authentication message is no longer valid.
    expiration_time: Option<String>,
    /// ISO 8601 datetime String that, if present, indicates when the signed
    /// authentication message will become valid.
    not_before: Option<String>,
    /// System-specific identifier that may be used to uniquely refer to the
    /// sign-in request.
    request_id: Option<String>,
    /// EIP-155 Chain ID to which the session is bound, and the network where
    /// Contract Accounts must be resolved.
    chain_id: Option<ChainId>,
    /// List of information or references to information the user wishes to have
    /// resolved as part of authentication by the relying party. They are
    /// expressed as RFC 3986 URIs separated by `\n- `.
    resources: Option<Vec<String>>,
    /// Signature of the message signed by the wallet.
    signature: Option<String>,
}

impl<'a> SiwxMessage<'a> {
    pub fn from_cacao(cacao: &'a Capability) -> Result<Self> {
        // fix
        let account = cacao.payload.issuer.replace("did:pkh:", "");
        let account =
            BlockchainAccountId::from_str(account.as_str()).context("cacao had invalid account")?;
        let address = account.account_address;
        let chain_id = Some(account.chain_id);

        Ok(Self {
            domain: &cacao.payload.domain,
            address,
            statement: cacao.payload.statement.clone(),
            uri: &cacao.payload.audience,
            version: &cacao.payload.version,
            nonce: &cacao.payload.nonce,
            issued_at: cacao
                .payload
                .issued_at
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            expiration_time: cacao
                .payload
                .expiration
                .map(|s| s.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            not_before: cacao
                .payload
                .not_before
                .map(|s| s.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
            request_id: cacao.payload.request_id.clone(),
            chain_id,
            resources: cacao.payload.resources.clone(),
            signature: Some(cacao.signature.signature.clone()),
        })
    }

    pub fn as_legacy_chain_id_message(&self, chain: &str) -> String {
        let mut msg = MessageBuilder::new(self, chain, &self.address);
        msg.add_uri_field();
        msg.add_version_field();
        msg.add_nonce_field();
        msg.add_iat_field();
        msg.add_exp_field_opt();
        msg.add_nbf_field_opt();
        msg.add_request_id_field_opt();
        msg.add_chain_id_field();
        msg.add_resources_field_opt();
        msg.build()
    }

    pub fn as_message_with_address(&self, chain: &str, address: &str) -> String {
        let mut msg = MessageBuilder::new(self, chain, address);

        msg.add_uri_field();
        msg.add_version_field();
        msg.add_chain_id_field();
        msg.add_nonce_field();
        msg.add_iat_field();
        msg.add_exp_field_opt();
        msg.add_nbf_field_opt();
        msg.add_request_id_field_opt();
        msg.add_resources_field_opt();
        msg.build()
    }

    pub fn as_message(&self, chain: &str) -> String {
        self.as_message_with_address(chain, &self.address)
    }
}

struct MessageBuilder<'a> {
    msg: &'a SiwxMessage<'a>,
    prefix: String,
    suffix: Vec<String>,
}

impl<'a> MessageBuilder<'a> {
    fn new(msg: &'a SiwxMessage, chain: &str, address: &str) -> Self {
        let prefix = [
            format!(
                "{} wants you to sign in with your {chain} account:",
                msg.domain
            )
            .as_str(),
            address,
        ]
        .join("\n");

        Self {
            msg,
            prefix,
            suffix: Vec::new(),
        }
    }

    fn add_uri_field(&mut self) {
        self.suffix.push(format!("URI: {}", self.msg.uri))
    }

    fn add_iat_field(&mut self) {
        self.suffix
            .push(format!("Issued At: {}", self.msg.issued_at))
    }

    fn add_version_field(&mut self) {
        self.suffix.push(format!("Version: {}", self.msg.version))
    }

    fn add_chain_id_field(&mut self) {
        if let Some(chain) = &self.msg.chain_id {
            self.suffix.push(format!("Chain ID: {}", chain.reference))
        }
    }
    fn add_nonce_field(&mut self) {
        self.suffix.push(format!("Nonce: {}", self.msg.nonce))
    }

    fn add_resources_field_opt(&mut self) {
        match &self.msg.resources {
            Some(v) if !v.is_empty() => {
                let r = v
                    .iter()
                    .map(|r| format!("- {r}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                self.suffix.push(format!("Resources:\n{}", r))
            }
            Some(_empty_vec) => {}
            None => {}
        }
    }

    fn add_exp_field_opt(&mut self) {
        if let Some(exp) = &self.msg.expiration_time {
            self.suffix.push(format!("Expiration Time: {}", exp));
        }
    }

    fn add_nbf_field_opt(&mut self) {
        if let Some(nbf) = &self.msg.not_before {
            self.suffix.push(format!("Not Before: {}", nbf))
        }
    }

    fn add_request_id_field_opt(&mut self) {
        if let Some(id) = &self.msg.request_id {
            self.suffix.push(format!("Request ID: {}", id))
        }
    }

    fn build(&self) -> String {
        let prefix = if let Some(ref statement) = self.msg.statement {
            // &String doesn't impl<'a> Borrow<str> for &'a String so you get
            // "method cannot be called on `[&String; 2]` due to unsatisfied trait bounds" without `as_str()`
            // rust-lang issue 82910
            &[self.prefix.as_str(), statement.as_str()].join("\n\n")
        } else {
            &self.prefix
        };
        let suffix = self.suffix.join("\n");

        [prefix.as_str(), suffix.as_str()].join("\n\n")
    }
}

#[cfg(test)]
mod test {

    use crate::test::CACAO_SIGNED_DATA_EVENT_CAR;

    use super::*;

    use base64::Engine as _;
    use ceramic_event::unvalidated;
    use ipld_core::ipld::Ipld;
    use test_log::test;

    #[test]
    fn test_siwx_messages() {
        let (_base, data) = multibase::decode(CACAO_SIGNED_DATA_EVENT_CAR).unwrap();
        let (_, event) = unvalidated::Event::<Ipld>::decode_car(data.as_slice(), false).unwrap();

        match event {
            unvalidated::Event::Time(_) => unreachable!("not a time event"),
            unvalidated::Event::Signed(s) => {
                let cap = s.capability().unwrap();
                let siwx = SiwxMessage::from_cacao(cap).unwrap();
                let expected_base64 = r#"dGVzdCB3YW50cyB5b3UgdG8gc2lnbiBpbiB3aXRoIHlvdXIgRXRoZXJldW0gYWNjb3VudDoKMHgzNzk0ZDRmMDc3YzA4ZDkyNWZmOGZmODIwMDA2YjczNTMyOTliMjAwCgpHaXZlIHRoaXMgYXBwbGljYXRpb24gYWNjZXNzIHRvIHNvbWUgb2YgeW91ciBkYXRhIG9uIENlcmFtaWMKClVSSTogZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAKVmVyc2lvbjogMQpDaGFpbiBJRDogMQpOb25jZTogd1BpQ09jcGtsbApJc3N1ZWQgQXQ6IDIwMjQtMDYtMTJUMjA6MDQ6NDIuNDY0WgpFeHBpcmF0aW9uIFRpbWU6IDIwMjQtMDYtMTlUMjA6MDQ6NDIuNDY0WgpSZXNvdXJjZXM6Ci0gY2VyYW1pYzovLyo="#;
                let expected = String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(expected_base64)
                        .unwrap(),
                )
                .unwrap();

                assert_eq!(expected, siwx.as_message("Ethereum"));

                let _eip55_base64 = r#"dGVzdCB3YW50cyB5b3UgdG8gc2lnbiBpbiB3aXRoIHlvdXIgRXRoZXJldW0gYWNjb3VudDoKMHgzNzk0ZDRmMDc3YzA4ZDkyNWZmOGZmODIwMDA2YjczNTMyOTliMjAwCgpHaXZlIHRoaXMgYXBwbGljYXRpb24gYWNjZXNzIHRvIHNvbWUgb2YgeW91ciBkYXRhIG9uIENlcmFtaWMKClVSSTogZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAKVmVyc2lvbjogMQpDaGFpbiBJRDogMQpOb25jZTogd1BpQ09jcGtsbApJc3N1ZWQgQXQ6IDIwMjQtMDYtMTJUMjA6MDQ6NDIuNDY0WgpFeHBpcmF0aW9uIFRpbWU6IDIwMjQtMDYtMTlUMjA6MDQ6NDIuNDY0WgpSZXNvdXJjZXM6Ci0gY2VyYW1pYzovLyo="#;
                let expected_legacy_base64 = r#"dGVzdCB3YW50cyB5b3UgdG8gc2lnbiBpbiB3aXRoIHlvdXIgRXRoZXJldW0gYWNjb3VudDoKMHgzNzk0ZDRmMDc3YzA4ZDkyNWZmOGZmODIwMDA2YjczNTMyOTliMjAwCgpHaXZlIHRoaXMgYXBwbGljYXRpb24gYWNjZXNzIHRvIHNvbWUgb2YgeW91ciBkYXRhIG9uIENlcmFtaWMKClVSSTogZGlkOmtleTp6Nk1rcjRhM1ozRkZhSkY4WXFXblduVkhxZDVlRGpaTjliRFNUNndWWkMxaEo4MVAKVmVyc2lvbjogMQpOb25jZTogd1BpQ09jcGtsbApJc3N1ZWQgQXQ6IDIwMjQtMDYtMTJUMjA6MDQ6NDIuNDY0WgpFeHBpcmF0aW9uIFRpbWU6IDIwMjQtMDYtMTlUMjA6MDQ6NDIuNDY0WgpDaGFpbiBJRDogMQpSZXNvdXJjZXM6Ci0gY2VyYW1pYzovLyo="#;
                let expected_legacy = String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(expected_legacy_base64)
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(expected_legacy, siwx.as_legacy_chain_id_message("Ethereum"));
            }
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        };
    }
}
