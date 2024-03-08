use anyhow::{anyhow, Result};

const MAINNET: u64 = 0x00;
const TESTNET_CLAY: u64 = 0x01;
const DEV_UNSTABLE: u64 = 0x02;
const IN_MEMORY: u64 = 0xff;
const LOCAL_OFFSET: u64 = 0x01_0000_0000;

/// Network values from https://cips.ceramic.network/tables/networkIds.csv
/// Ceramic Pubsub Topic, Timestamp Authority
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Network {
    /// Production network
    Mainnet,
    /// Test network
    TestnetClay,
    /// Developement network
    DevUnstable,
    /// Local network with unique id
    Local(u32),
    /// Singleton network in memory
    InMemory,
}

impl Network {
    /// Get the network as an integer id.
    pub fn try_from_id(id: u64) -> Result<Self> {
        match id {
            MAINNET => Ok(Network::Mainnet),
            TESTNET_CLAY => Ok(Network::TestnetClay),
            DEV_UNSTABLE => Ok(Network::DevUnstable),
            IN_MEMORY => Ok(Network::InMemory),
            id if id > LOCAL_OFFSET => Ok(Network::Local((id - LOCAL_OFFSET) as u32)),
            id => Err(anyhow!("unknown network id: {}", id)),
        }
    }
    /// Get the network as an integer id.
    pub fn id(&self) -> u64 {
        match self {
            // https://github.com/ceramicnetwork/CIPs/blob/main/tables/networkIds.csv
            Network::Mainnet => MAINNET,
            Network::TestnetClay => TESTNET_CLAY,
            Network::DevUnstable => DEV_UNSTABLE,
            Network::InMemory => IN_MEMORY,
            Network::Local(id) => LOCAL_OFFSET + *id as u64,
        }
    }

    /// Get the network as a unique name.
    pub fn name(&self) -> String {
        match self {
            Network::Mainnet => "/ceramic".to_owned(),
            Network::TestnetClay => "/ceramic/testnet-clay".to_owned(),
            Network::DevUnstable => "/ceramic/dev-unstable".to_owned(),
            Network::Local(i) => format!("/ceramic/local-{}", i),
            Network::InMemory => "/ceramic/inmemory".to_owned(),
        }
    }
}
impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
