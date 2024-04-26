mod block;
mod event;
mod query;

pub use block::{BlockBytes, BlockRow};
pub use event::{
    rebuild_car, CountRow, DeliveredEvent, EventBlockRaw, EventRaw, OrderKey, ReconHash,
};
pub use query::{BlockQuery, EventBlockQuery, EventQuery, ReconQuery, ReconType, SqlBackend};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FirstAndLast {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}
