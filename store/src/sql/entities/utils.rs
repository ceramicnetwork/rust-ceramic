use ceramic_core::EventId;

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
