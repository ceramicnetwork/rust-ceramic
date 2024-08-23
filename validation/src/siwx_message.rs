use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ssi::caip10::BlockchainAccountId;

// use ssi_caips::{caip10::BlockchainAccountId, caip2::ChainId};
use crate::cacao::Capability;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SiwxMessage<'a> {
    ///RFC 4501 dns authority that is requesting the signing.
    domain: &'a str,
    /// The Account ID as specified by CAIP-10.
    /// This is used to display the `Address` and `ChainID` fields to the user.
    ///
    /// The `Address` is the address performing the signing conformant to capitalization
    /// encoded checksum specified in EIP-55 where applicable if Ethereum.
    /// which is the `acccount_address` field from the `BlockchainAccountId`.
    ///
    /// The `ChainID` is the Chain ID (CAIP-2) to which the session is bound, and the
    /// network where Contract Accounts must be resolved (e.g. EIP-155). Presented as
    /// the `reference` portion of the chain ID.
    account: BlockchainAccountId,
    /// Human-readable ASCII assertion that the user will sign, and it must not
    /// contain `\n`.
    statement: Option<&'a str>,
    /// RFC 3986 URI referring to the resource that is the subject of the signing
    /// (as in the __subject__ of a claim).
    uri: &'a str,
    /// Current version of the message.
    version: &'a str,
    /// Randomized token used to prevent replay attacks, at least 8 alphanumeric
    /// characters.
    nonce: &'a str,
    /// ISO 8601 datetime String of the current time.
    issued_at: DateTime<Utc>,
    /// Presented to the user as an ISO 8601 datetime String that, if present,
    /// indicates when the signed authentication message is no longer valid.
    expiration_time: Option<DateTime<Utc>>,
    /// Presented to the user as a ISO 8601 datetime String that, if present,
    /// indicates when the signed authentication message will become valid.
    not_before: Option<DateTime<Utc>>,
    /// System-specific identifier that may be used to uniquely refer to the
    /// sign-in request.
    request_id: Option<&'a str>,
    /// List of information or references to information the user wishes to have
    /// resolved as part of authentication by the relying party. They are
    /// expressed as RFC 3986 URIs separated by `\n- `.
    resources: Option<&'a [String]>,
    /// Signature of the message signed by the wallet.
    signature: Option<&'a str>,
}

impl<'a> SiwxMessage<'a> {
    pub fn from_cacao(cacao: &'a Capability) -> Result<Self> {
        let account = cacao.payload.issuer.replace("did:pkh:", "");
        let account =
            BlockchainAccountId::from_str(account.as_str()).context("cacao had invalid account")?;

        Ok(Self {
            domain: &cacao.payload.domain,
            account,
            statement: cacao.payload.statement.as_deref(),
            uri: &cacao.payload.audience,
            version: &cacao.payload.version,
            nonce: &cacao.payload.nonce,
            issued_at: cacao.payload.issued_at()?,
            expiration_time: cacao.payload.expiration()?,
            not_before: cacao.payload.not_before()?,
            request_id: cacao.payload.request_id.as_deref(),
            resources: cacao.payload.resources.as_deref(),
            signature: Some(cacao.signature.signature.as_ref()),
        })
    }

    pub fn as_legacy_chain_id_message(&self, chain: &str) -> String {
        let mut msg = MessageBuilder::new(self, chain, &self.account.account_address);
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
        self.as_message_with_address(chain, &self.account.account_address)
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
        self.suffix.push(format!(
            "Issued At: {}",
            self.msg
                .issued_at
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        ))
    }

    fn add_version_field(&mut self) {
        self.suffix.push(format!("Version: {}", self.msg.version))
    }

    fn add_chain_id_field(&mut self) {
        // we use the chain ID namespace in the prefix message and the reference in this field
        self.suffix
            .push(format!("Chain ID: {}", self.msg.account.chain_id.reference))
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
            self.suffix.push(format!(
                "Expiration Time: {}",
                exp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));
        }
    }

    fn add_nbf_field_opt(&mut self) {
        if let Some(nbf) = &self.msg.not_before {
            self.suffix.push(format!(
                "Not Before: {}",
                nbf.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ))
        }
    }

    fn add_request_id_field_opt(&mut self) {
        if let Some(id) = &self.msg.request_id {
            self.suffix.push(format!("Request ID: {}", id))
        }
    }

    fn build(&self) -> String {
        let prefix = if let Some(statement) = self.msg.statement {
            // with one &str you only need a borrow, but with both &String references you get
            // &String doesn't impl<'a> Borrow<str> for &'a String
            // "method cannot be called on `[&String; 2]` due to unsatisfied trait bounds"
            // rust-lang issue 82910 -> use `.as_str()` instead of borrowing
            &[&self.prefix, statement].join("\n\n")
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
