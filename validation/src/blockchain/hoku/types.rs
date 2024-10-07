use std::str::FromStr as _;

use ceramic_event::unvalidated::Proof;
use hoku_sdk::address::Address;

use crate::blockchain::Error;

const HOKU_TX_TYPE: &str = "hoku(address:index)";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Input for resolving a time event proof from a hoku accumulator contract
pub struct HokuTxInput {
    /// The index in the accumulator of the leaf node
    pub index: u64,
    /// The address of the accumulator contract
    pub address: hoku_sdk::address::Address,
}

// {
//   "chainId": "hoku:mainnet:",
//   "root": {"/": "bafyreicbwzaiyg2l4uaw6zjds3xupqeyfq3nlb36xodusgn24ou3qvgy4e"},
//   "txHash": {"/": ".."},
//   "txType": "hoku(address:index)"
//   "accumulator": "todo(address:index)"
// }
impl TryFrom<&Proof> for HokuTxInput {
    type Error = Error;

    fn try_from(value: &Proof) -> Result<Self, Self::Error> {
        match value.tx_type() {
            HOKU_TX_TYPE => {
                let (address, index) = value.accumulator().split_once(":").ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "Hoku time event has invalid accumulator proof format: {}",
                        value.accumulator()
                    ))
                })?;

                let index: u64 = index.parse().map_err(|e| Error::InvalidArgument(format!("Proof of type {HOKU_TX_TYPE} must have integer value as second piece instead of {index}. Error: {e}")))?;
                let address = Address::from_str(address).map_err(|e| {
                    Error::InvalidArgument(format!("Failed to parse address {address}: {e}"))
                })?;
                Ok(Self { address, index })
            }
            v => Err(Error::InvalidArgument(format!(
                "Unable to parse tx_type: {}",
                v
            ))),
        }
    }
}
