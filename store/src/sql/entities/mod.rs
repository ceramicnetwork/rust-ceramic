mod block;
mod event;
mod event_block;
mod hash;
mod interest;
mod query;
mod utils;

pub use block::{BlockBytes, BlockRow};
pub use event::{rebuild_car, EventRaw};
pub use event_block::EventBlockRaw;
pub use hash::{BlockHash, ReconHash};
pub use interest::CeramicOneInterest;
pub use query::{BlockQuery, EventBlockQuery, EventQuery, ReconQuery, ReconType, SqlBackend};
pub use utils::{CountRow, DeliveredEvent, OrderKey};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FirstAndLast {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}
