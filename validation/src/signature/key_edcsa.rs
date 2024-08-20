#[cfg(test)]
mod test {

    use test_log::test;

    use crate::test::{get_test_event, verify_event_envelope};

    #[test]
    fn deterministic_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::EcdsaP256,
            crate::test::TestEventType::DeterministicInit,
        );
    }

    #[test(tokio::test)]
    async fn data_validates() {
        verify_event_envelope(
            crate::test::SigningType::EcdsaP256,
            crate::test::TestEventType::SignedData,
        )
        .await;
    }

    #[test(tokio::test)]
    async fn init_validates() {
        verify_event_envelope(
            crate::test::SigningType::EcdsaP256,
            crate::test::TestEventType::SignedInit,
        )
        .await;
    }
}
