#[cfg(test)]
mod test {

    use test_log::test;

    use crate::test::get_test_event;

    #[test]
    fn deterministic_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::Ed2559,
            crate::test::TestEventType::DeterministicInit,
        );
    }

    #[test]
    fn data_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::Ed2559,
            crate::test::TestEventType::SignedData,
        );
    }

    #[test]
    fn init_generates() {
        let (_cid, _event) = get_test_event(
            crate::test::SigningType::Ed2559,
            crate::test::TestEventType::SignedInit,
        );
    }
}
