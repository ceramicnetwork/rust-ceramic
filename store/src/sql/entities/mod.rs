mod block;
mod event;
mod event_block;
mod hash;
mod utils;

pub use block::{BlockBytes, BlockRow};
pub use event::{rebuild_car, EventRaw};
pub use event_block::EventBlockRaw;
pub use hash::{BlockHash, ReconHash};

pub use utils::{CountRow, DeliveredEvent, OrderKey};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FirstAndLast {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}
