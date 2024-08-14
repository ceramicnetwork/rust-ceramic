//! # Ceramic Event
//! Implementation of ceramic event protocol, with appropriate compatibilility with js-ceramic
#![warn(missing_docs)]

/// Anchor-related types
pub mod anchor;
mod bytes;
/// Unvalidated event types
pub mod unvalidated;

pub use ceramic_core::*;

#[cfg(test)]
pub mod tests {
    use ceramic_core::DidDocument;

    use crate::unvalidated::signed::JwkSigner;

    pub fn to_pretty_json(json_data: &[u8]) -> String {
        let json: serde_json::Value = match serde_json::from_slice(json_data) {
            Ok(r) => r,
            Err(_) => {
                panic!(
                    "input data should be valid json: {:?}",
                    String::from_utf8(json_data.to_vec())
                )
            }
        };
        serde_json::to_string_pretty(&json).unwrap()
    }

    pub fn serialize_to_pretty_json<T: serde::Serialize>(data: &T) -> String {
        serde_json::to_string_pretty(data).unwrap()
    }

    pub async fn signer() -> JwkSigner {
        JwkSigner::new(
            DidDocument::new("did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw"),
            "810d51e02cb63066b7d2d2ec67e05e18c29b938412050bdd3c04d878d8001f3c",
        )
        .await
        .unwrap()
    }
}
