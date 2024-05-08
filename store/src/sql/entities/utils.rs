use ceramic_core::EventId;
use cid::Cid;

use crate::{Error, Result};

#[derive(Debug, sqlx::FromRow)]
/// The query returns a column 'res' that is an int8
/// Be careful on postgres as some operations return int4 e.g. length()
pub struct CountRow {
    pub res: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderKey {
    pub order_key: Vec<u8>,
}

impl TryFrom<OrderKey> for EventId {
    type Error = ceramic_core::event_id::InvalidEventId;

    fn try_from(value: OrderKey) -> std::prelude::v1::Result<Self, Self::Error> {
        EventId::try_from(value.order_key)
    }
}

#[derive(sqlx::FromRow)]
pub struct DeliveredEvent {
    pub cid: Vec<u8>,
    pub new_highwater_mark: i64,
}

impl DeliveredEvent {
    /// assumes rows are sorted by `delivered` ascending
    pub fn parse_query_results(current: i64, rows: Vec<Self>) -> Result<(i64, Vec<Cid>)> {
        let max: i64 = rows.last().map_or(current, |r| r.new_highwater_mark + 1);
        let rows = rows
            .into_iter()
            .map(|row| Cid::try_from(row.cid).map_err(Error::new_app))
            .collect::<Result<Vec<Cid>>>()?;

        Ok((max, rows))
    }
}
