use async_trait::async_trait;
use mockall::mock;

use crate::ConclusionEvent;

mock! {
    #[derive(Debug)]
    pub ConclusionFeed {}
    #[async_trait]
    impl crate::ConclusionFeed for ConclusionFeed {
        async fn conclusion_events_since(
            &self,
            highwater_mark: i64,
            limit: i64,
        ) -> anyhow::Result<Vec<ConclusionEvent>>;
    }
}
