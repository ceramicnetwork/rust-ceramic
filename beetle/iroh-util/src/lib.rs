use cid::{
    multihash::{Code, MultihashDigest},
    Cid,
};

/// Verifies that the provided bytes hash to the given multihash.
pub fn verify_hash(cid: &Cid, bytes: &[u8]) -> Option<bool> {
    Code::try_from(cid.hash().code()).ok().map(|code| {
        let calculated_hash = code.digest(bytes);
        &calculated_hash == cid.hash()
    })
}
