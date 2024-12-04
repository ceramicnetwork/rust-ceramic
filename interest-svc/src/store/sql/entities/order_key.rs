use ceramic_core::Interest;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderKey {
    pub order_key: Vec<u8>,
}

impl TryFrom<OrderKey> for Interest {
    type Error = ceramic_core::interest::InvalidInterest;

    fn try_from(value: OrderKey) -> std::prelude::v1::Result<Self, Self::Error> {
        Interest::try_from(value.order_key)
    }
}
