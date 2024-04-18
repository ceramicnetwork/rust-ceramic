//! Testing utilities for recon
use crate::{
    protocol::Recon as ProtocolRecon, HashCount, InsertResult,
    InterestProvider as ReconInterestProvider, Metrics, ReconItem, Result, Sha256a, Store,
    SyncState,
};
use ceramic_core::{EventId, Interest, RangeOpen};
use mockall::mock;

type MockSyncState<T> = SyncState<<T as ProtocolRecon>::Key, Sha256a>;

mock! {

    pub EventIdRecon {}

    impl Clone for EventIdRecon {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl ProtocolRecon for EventIdRecon {
        type Key = EventId;
        type Hash = Sha256a;
        /// Insert a new key into the key space.
        async fn insert(&self, key: <MockEventIdRecon as ProtocolRecon>::Key, value: Option<Vec<u8>>) -> Result<()>;

        /// Get all keys in the specified range
        async fn range(
            &self,
            left_fencepost: <MockEventIdRecon as ProtocolRecon>::Key,
            right_fencepost: <MockEventIdRecon as ProtocolRecon>::Key,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<<MockEventIdRecon as ProtocolRecon>::Key>>;

        /// Reports total number of keys
        async fn len(&self) -> Result<usize>;

        /// Reports if the set is empty.
        async fn is_empty(&self) -> Result<bool> {
            Ok(self.len().await? == 0)
        }

        /// Retrieve a value associated with a recon key
        async fn value_for_key(&self, key: <MockEventIdRecon as ProtocolRecon>::Key) -> Result<Option<Vec<u8>>>;

        /// Report all keys in the range that are missing a value
        async fn keys_with_missing_values(&self, range: RangeOpen<<MockEventIdRecon as ProtocolRecon>::Key>)
            -> Result<Vec<<MockEventIdRecon as ProtocolRecon>::Key>>;

        /// Reports the interests of this recon instance
        async fn interests(&self) -> Result<Vec<RangeOpen<<MockEventIdRecon as ProtocolRecon>::Key>>>;

        /// Computes the intersection of input interests with the local interests
        async fn process_interests(
            &self,
            interests: Vec<RangeOpen<<MockEventIdRecon as ProtocolRecon>::Key>>,
        ) -> Result<Vec<RangeOpen<<MockEventIdRecon as ProtocolRecon>::Key>>>;

        /// Compute an initial hash for the range
        async fn initial_range(
            &self,
            interest: RangeOpen<<MockEventIdRecon as ProtocolRecon>::Key>,
        ) -> Result<crate::Range<<MockEventIdRecon as ProtocolRecon>::Key, Sha256a>>;

        /// Computes a response to a remote range
        async fn process_range(
            &self,
            range: crate::Range<<MockEventIdRecon as ProtocolRecon>::Key, Sha256a>,
        ) -> Result<(MockSyncState<MockEventIdRecon>, Vec<<MockEventIdRecon as ProtocolRecon>::Key>)>;

        /// Create a handle to the metrics
        fn metrics(&self) -> Metrics;
    }
}

/// Expose mock for recon with event id
pub type MockReconForEventId = MockEventIdRecon;

mock! {

    pub InterestRecon {}

    impl Clone for InterestRecon {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl ProtocolRecon for InterestRecon {
        type Key = Interest;
        type Hash = Sha256a;
        /// Insert a new key into the key space.
        async fn insert(&self, key: <MockInterestRecon as ProtocolRecon>::Key, value: Option<Vec<u8>>) -> Result<()>;

        /// Get all keys in the specified range
        async fn range(
            &self,
            left_fencepost: <MockInterestRecon as ProtocolRecon>::Key,
            right_fencepost: <MockInterestRecon as ProtocolRecon>::Key,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<<MockInterestRecon as ProtocolRecon>::Key>>;

        /// Reports total number of keys
        async fn len(&self) -> Result<usize>;

        /// Reports if the set is empty.
        async fn is_empty(&self) -> Result<bool> {
            Ok(self.len().await? == 0)
        }

        /// Retrieve a value associated with a recon key
        async fn value_for_key(&self, key: <MockInterestRecon as ProtocolRecon>::Key) -> Result<Option<Vec<u8>>>;

        /// Report all keys in the range that are missing a value
        async fn keys_with_missing_values(&self, range: RangeOpen<<MockInterestRecon as ProtocolRecon>::Key>)
            -> Result<Vec<<MockInterestRecon as ProtocolRecon>::Key>>;

        /// Reports the interests of this recon instance
        async fn interests(&self) -> Result<Vec<RangeOpen<<MockInterestRecon as ProtocolRecon>::Key>>>;

        /// Computes the intersection of input interests with the local interests
        async fn process_interests(
            &self,
            interests: Vec<RangeOpen<<MockInterestRecon as ProtocolRecon>::Key>>,
        ) -> Result<Vec<RangeOpen<<MockInterestRecon as ProtocolRecon>::Key>>>;

        /// Compute an initial hash for the range
        async fn initial_range(
            &self,
            interest: RangeOpen<<MockInterestRecon as ProtocolRecon>::Key>,
        ) -> Result<crate::Range<<MockInterestRecon as ProtocolRecon>::Key, Sha256a>>;

        /// Computes a response to a remote range
        async fn process_range(
            &self,
            range: crate::Range<<MockInterestRecon as ProtocolRecon>::Key, Sha256a>,
        ) -> Result<(MockSyncState<MockInterestRecon>, Vec<<MockInterestRecon as ProtocolRecon>::Key>)>;

        /// Create a handle to the metrics
        fn metrics(&self) -> Metrics;
    }
}

/// Expose mock for recon with interest
pub type MockReconForInterest = MockInterestRecon;

mock! {
    pub InterestInterestProvider {}

    impl Clone for InterestInterestProvider {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl ReconInterestProvider for InterestInterestProvider {
        type Key = Interest;

        async fn interests(&self) -> Result<Vec<RangeOpen<Interest>>>;
    }
}

/// Expose mock for interest provider with interest
pub type MockInterestProviderForInterest = MockInterestInterestProvider;

mock! {
    pub EventIdInterestProvider {}

    impl Clone for EventIdInterestProvider {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl ReconInterestProvider for EventIdInterestProvider {
        type Key = EventId;

        async fn interests(&self) -> Result<Vec<RangeOpen<EventId>>>;
    }
}

/// Expose mock for interest provider with event id
pub type MockInterestProviderForEventId = MockEventIdInterestProvider;

mock! {
    pub EventIdStore {}

    impl Clone for EventIdStore {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl Store for EventIdStore {
        type Key = EventId;
        type Hash = Sha256a;

        async fn insert<'a>(&self, item: &ReconItem<'a, EventId>) -> Result<bool>;

        async fn insert_many<'a>(&self, items: &[ReconItem<'a, EventId>]) -> Result<InsertResult>;

        async fn hash_range(
            &self,
            left_fencepost: &EventId,
            right_fencepost: &EventId,
        ) -> Result<HashCount<Sha256a>>;

        async fn range(
            &self,
            left_fencepost: &EventId,
            right_fencepost: &EventId,
            offset: usize,
            limit: usize,
        ) -> Result<Box<dyn Iterator<Item = EventId> + Send + 'static>>;

        async fn range_with_values(
            &self,
            left_fencepost: &EventId,
            right_fencepost: &EventId,
            offset: usize,
            limit: usize,
        ) -> Result<Box<dyn Iterator<Item = (EventId, Vec<u8>)> + Send + 'static>>;

        async fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;

        async fn keys_with_missing_values(&self, range: RangeOpen<EventId>)
            -> Result<Vec<EventId>>;
    }
}

/// Expose mock for store with event id
pub type MockStoreForEventId = MockEventIdStore;

mock! {
    pub InterestStore {}

    impl Clone for InterestStore {
        fn clone(&self) -> Self;
    }

    #[async_trait::async_trait]
    impl Store for InterestStore {
        type Key = Interest;
        type Hash = Sha256a;

        async fn insert<'a>(&self, item: &ReconItem<'a, Interest>) -> Result<bool>;

        async fn insert_many<'a>(&self, items: &[ReconItem<'a, Interest>]) -> Result<InsertResult>;

        async fn hash_range(
            &self,
            left_fencepost: &Interest,
            right_fencepost: &Interest,
        ) -> Result<HashCount<Sha256a>>;

        async fn range(
            &self,
            left_fencepost: &Interest,
            right_fencepost: &Interest,
            offset: usize,
            limit: usize,
        ) -> Result<Box<dyn Iterator<Item = Interest> + Send + 'static>>;

        async fn range_with_values(
            &self,
            left_fencepost: &Interest,
            right_fencepost: &Interest,
            offset: usize,
            limit: usize,
        ) -> Result<Box<dyn Iterator<Item = (Interest, Vec<u8>)> + Send + 'static>>;

        async fn value_for_key(&self, key: &Interest) -> Result<Option<Vec<u8>>>;

        async fn keys_with_missing_values(&self, range: RangeOpen<Interest>)
            -> Result<Vec<Interest>>;
    }
}

/// Expose mock for store with event id
pub type MockStoreForInterest = MockInterestStore;
