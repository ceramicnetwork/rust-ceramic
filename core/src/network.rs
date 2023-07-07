/// Network values from https://cips.ceramic.network/tables/networkIds.csv
/// Ceramic Pubsub Topic, Timestamp Authority
#[derive(Debug, Clone)]
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
    pub fn id(&self) -> u64 {
        match self {
            // https://github.com/ceramicnetwork/CIPs/blob/main/tables/networkIds.csv
            Network::Mainnet => 0x00,
            Network::TestnetClay => 0x01,
            Network::DevUnstable => 0x02,
            Network::InMemory => 0xff,
            Network::Local(id) => 0x01_0000_0000_u64 + u64::from(*id),
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
