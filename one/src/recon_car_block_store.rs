use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_p2p::SQLiteBlockStore;
use iroh_car::CarReader;
use recon::{HashCount, InsertResult, ReconItem, Store};

// Implements the Recon Store trait and assumes values are Car files containing blocks and stores
// them in the block store
#[derive(Debug)]
pub struct ReconCarBlockStore<S> {
    store: S,
    block_store: SQLiteBlockStore,
}

impl<S> ReconCarBlockStore<S> {
    pub fn new(store: S, block_store: SQLiteBlockStore) -> Self {
        Self { store, block_store }
    }
    async fn store_car_blocks(&mut self, car: &[u8]) -> Result<()> {
        let mut reader = CarReader::new(car).await?;
        while let Some((cid, bytes)) = reader.next_block().await? {
            self.block_store.put(cid, bytes.into(), vec![]).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<S> Store for ReconCarBlockStore<S>
where
    S: Store + Send,
{
    type Key = S::Key;
    type Hash = S::Hash;

    async fn insert(&mut self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        if let Some(value) = item.value {
            self.store_car_blocks(value).await?
        }
        self.store.insert(item).await
    }

    async fn insert_many<'a, I>(&mut self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, Self::Key>> + Send + Sync,
    {
        let items: Vec<ReconItem<_>> = items.collect();
        for item in &items {
            if let Some(value) = item.value {
                self.store_car_blocks(value).await?
            }
        }

        self.store.insert_many(items.into_iter()).await
    }
    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        self.store.hash_range(left_fencepost, right_fencepost).await
    }

    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.store
            .range(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn range_with_values(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.store
            .range_with_values(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn full_range(&mut self) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.store.full_range().await
    }

    async fn middle(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        self.store.middle(left_fencepost, right_fencepost).await
    }
    async fn count(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        self.store.count(left_fencepost, right_fencepost).await
    }
    async fn first(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        self.store.first(left_fencepost, right_fencepost).await
    }
    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        self.store.last(left_fencepost, right_fencepost).await
    }

    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        self.store
            .first_and_last(left_fencepost, right_fencepost)
            .await
    }

    async fn len(&mut self) -> Result<usize> {
        self.store.len().await
    }
    async fn is_empty(&mut self) -> Result<bool> {
        self.store.is_empty().await
    }

    async fn value_for_key(&mut self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.store.value_for_key(key).await
    }

    async fn keys_with_missing_values(
        &mut self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        self.store.keys_with_missing_values(range).await
    }
}
