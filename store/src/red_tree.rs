use std::{
    marker::PhantomData,
    ops::{Add, AddAssign, Range},
    sync::Arc,
};

use anyhow::Result;
use async_recursion::async_recursion;
use async_trait::async_trait;
use ceramic_core::EventId;
use cid::Cid;
use iroh_bitswap::Block;
use itertools::Itertools;
use recon::{HashCount, InsertResult, ReconItem, Sha256a};
use tokio::sync::RwLock;
use tracing::debug;

/// Red(uction)Tree is a variant of BTree that is only the top portion of a btree.
/// The tree is in memory and can serve queries about the large ranges of the tree but delegates to
/// another implementation for more fine grained ranges of the tree.
///
/// It is named a Reduction Tree because the only query supported is a reduction of values within a
/// range. The reduction of the value must be associative and commutative so that it can be incrementally reduced and
/// stored on intermediate nodes.
#[derive(Debug, Clone)]
pub struct RedTree<K, V, S> {
    inner: Arc<RwLock<RedTreeInner<K, V, S>>>,
}

impl<K, V, S> RedTree<K, V, S>
where
    S: recon::Store<Key = K, Hash = V> + Send + Sync,
    K: recon::Key,
    V: recon::AssociativeHash,
    V: for<'a> From<&'a K>,
    V: for<'a> AddAssign<&'a V>,
    V: AddAssign<V>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default + Clone,
{
    /// Construct a new RedTree, this will immediately read any existing data from the store and
    /// populate the tree.
    pub async fn new(b: usize, bucket_size: u64, store: S) -> Result<Self> {
        Ok(RedTree {
            inner: Arc::new(RwLock::new(
                RedTreeInner::builder()
                    .with_b(b)
                    .with_bucket_size(bucket_size)
                    .build(store)
                    .await?,
            )),
        })
    }
}

struct RedTreeInner<K, V, S> {
    root: Option<Node<K, V>>,
    b: usize,
    bucket_size: u64,
    store: S,
}

impl<K, V, S> std::fmt::Debug for RedTreeInner<K, V, S>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedTreeInner")
            .field("root", &self.root)
            .field("b", &self.b)
            .field("bucket_size", &self.bucket_size)
            .field("store", &"<>")
            .finish()
    }
}

// TODO do we want this builder pattern?
struct Builder<K, V, S> {
    b: usize,
    bucket_size: u64,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    _s: PhantomData<S>,
}

impl<S, K, V> Builder<S, K, V>
where
    S: recon::Store<Key = K, Hash = V> + Send + Sync,
    K: recon::Key,
    V: recon::AssociativeHash,
    V: for<'a> From<&'a K>,
    V: for<'a> AddAssign<&'a V>,
    V: AddAssign<V>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default + Clone,
{
    fn with_b(self, b: usize) -> Self {
        Self { b, ..self }
    }
    fn with_bucket_size(self, bucket_size: u64) -> Self {
        Self {
            bucket_size,
            ..self
        }
    }
    /// Constructs the tree from any data in the provided store.
    async fn build(self, store: S) -> Result<RedTreeInner<K, V, S>> {
        let mut tree = RedTreeInner {
            root: None,
            store,
            b: self.b,
            bucket_size: self.bucket_size,
        };
        tree.root = Some(
            tree.build_root_node(&K::min_value()..&K::max_value())
                .await?,
        );
        Ok(tree)
    }
}

#[derive(Clone, Debug)]
pub struct Entry<K, V> {
    key: K,
    value: V,
}

impl<K, V, S> RedTreeInner<K, V, S> {
    /// Create a builder for constructing a RedTree.
    pub fn builder() -> Builder<K, V, S> {
        Builder {
            b: 16,
            bucket_size: 2_u64.pow(15),
            _k: Default::default(),
            _v: Default::default(),
            _s: Default::default(),
        }
    }
}
impl<K, V, S> RedTreeInner<K, V, S>
where
    S: recon::Store<Key = K, Hash = V> + Send + Sync,
    K: recon::Key,
    V: recon::AssociativeHash,
    V: for<'a> From<&'a K>,
    V: for<'a> AddAssign<&'a V>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default + Clone,
{
    fn size(&self) -> usize {
        self.b * 2 - 1
    }
    async fn build_root_node<'a>(&'a mut self, range: Range<&'a K>) -> Result<Node<K, V>> {
        // Compute a balanced depth of the tree once, then use the depth to build a balanced tree.
        //
        // Computation derivation:
        //
        //  * leaf_capacity - The number of values a leaf can store
        //  * n - The number of splits at each node
        //  * count - The total number of values in the range
        //  * depth - The depth of the tree
        //
        //  We need to choose a depth such that:
        //
        //  (count / n ^ depth) < leaf_capacity
        //
        //  i.e. the number of values in a node at depth fits inside a single leaf.
        //
        // Solving for depth we get:
        //
        // depth = log_n(count / leaf_capacity)
        //
        // Then we round depth up to the nearest integer.

        // Compute a leaf_capacity that is half full so buckets can fill up before needing to split
        let leaf_capacity = self.b as u64 * self.bucket_size;
        // We assume a split of 2, need to figure out how to not make this a constant
        const N: f64 = 2.0;

        let count = self.store.count(range.clone()).await?;
        let depth = (((count as f64) / (leaf_capacity as f64)).log(N)).ceil() as usize;
        debug!(
            self.b,
            self.bucket_size, leaf_capacity, count, depth, "build_root_node"
        );
        // Now that we know the depth simply recurse to that depth building a tree.
        // A leaf has depth=0 and the root's depth is depth.
        self.build_node(depth, range).await
    }

    // Recursively build nodes of the tree.
    // This relies on the split implementation of the store to produce even splits.
    // Otherwise an unbalanced tree will be constructed.
    // TODO can we be robust to this?
    #[async_recursion]
    async fn build_node<'a>(&'a mut self, depth: usize, range: Range<&'a K>) -> Result<Node<K, V>> {
        let mut split = self.store.split(range.clone()).await?;
        debug!(depth, ?split, "build_node");
        let size = self.size();
        if depth > 0 {
            // We must construct an internal node
            let mut keys = Vec::with_capacity(size + 1);
            keys.push(range.start.clone());
            keys.append(&mut split.keys);
            keys.push(range.end.clone());

            let mut children = Vec::with_capacity(size);
            let mut values = Vec::with_capacity(size);

            for (start, end) in keys.iter().tuple_windows() {
                let child = self.build_node(depth - 1, start..end).await?;
                values.push(child.reduce());
                children.push(child);
            }

            debug_assert_eq!(keys.len(), children.len() + 1);
            debug_assert_eq!(children.len(), values.len());

            Ok(Node::Internal(Internal {
                keys,
                children,
                values,
            }))
        } else {
            // We construct a single leaf node
            let mut keys = Vec::with_capacity(size + 1);
            keys.push(range.start.clone());
            keys.append(&mut split.keys);
            keys.push(range.end.clone());
            let leaf = Leaf {
                keys,
                buckets: split.hashes.into_iter().map(Bucket::from).collect(),
            };
            debug_assert_eq!(leaf.keys.len(), leaf.buckets.len() + 1);
            Ok(Node::Leaf(leaf))
        }
    }
    /// Insert a ReconItem into the store
    /// Insert a key value pair possibly splitting nodes along the way.
    async fn insert_hash(&mut self, kv: &Entry<K, V>) -> Result<()> {
        debug!(?kv, "insert_hash");
        if let Some(mut root) = self.root.take() {
            if self.is_node_full(&root) {
                // split the root creating a new root and child nodes along the way.
                let (median, sibling) = root.split(self.b);
                let root_value = root.reduce();
                let sibling_value = sibling.reduce();
                let size = self.size();
                let mut new_root = Internal {
                    keys: Vec::with_capacity(size + 1),
                    children: Vec::with_capacity(size),
                    values: Vec::with_capacity(size),
                };
                new_root.keys.push(K::min_value());
                new_root.keys.push(median);
                new_root.keys.push(K::max_value());
                new_root.children.push(root);
                new_root.children.push(sibling);
                new_root.values.push(root_value);
                new_root.values.push(sibling_value);
                debug_assert_eq!(new_root.keys.len(), new_root.children.len() + 1);
                debug_assert_eq!(new_root.children.len(), new_root.values.len());
                root = Node::Internal(new_root);
            }
            let ret = self.insert_non_full(&mut root, kv).await;
            self.root = Some(root);
            ret
        } else {
            let mut root = Node::Leaf(Leaf::new(self.b, (K::min_value(), K::max_value())));
            let ret = self.insert_non_full(&mut root, kv).await;
            self.root = Some(root);
            ret
        }
    }

    /// insert_non_full (recursively) finds a node rooted at a given non-full node.
    /// to insert a given key-value pair.
    #[async_recursion]
    async fn insert_non_full(&mut self, node: &mut Node<K, V>, kv: &Entry<K, V>) -> Result<()> {
        match node {
            Node::Internal(node) => {
                // Returns the index of the first key that is greater than kv.key
                let key_idx = node.keys.partition_point(|key| key <= &kv.key);
                debug_assert!(
                    key_idx < node.keys.len(),
                    "should not get insert request for key outside bounds of internal node: key={} min={}, max={}",
                    &kv.key,
                    node.keys[0],
                    node.keys[node.keys.len() - 1],
                );
                // Translate the key index into an index into the children vector.
                let child_idx = key_idx - 1;
                let child = &mut node.children[child_idx];
                if self.is_node_full(&child) {
                    let (median, mut sibling) = child.split(self.b);

                    // Recurse
                    let ret = if kv.key < median {
                        self.insert_non_full(child, kv).await
                    } else {
                        self.insert_non_full(&mut sibling, kv).await
                    };

                    // Now that both child and sibling are updated we can update the current node.
                    // Update/Insert values for new children
                    node.values[child_idx] = child.reduce();

                    // Siblings keys are greater than the original child thus need to be inserted
                    // at the next index.
                    node.values.insert(child_idx + 1, sibling.reduce());
                    node.children.insert(child_idx + 1, sibling);

                    // Insert new median key
                    node.keys.insert(key_idx, median.clone());

                    ret
                } else {
                    // Child is not full, simply recurse and update its value
                    let ret = self.insert_non_full(child, &kv).await;
                    if ret.is_ok() {
                        // TODO: This is almost certainly not enough error recovery protection.
                        // We will need to build a more robust _transaction_ into the red tree.
                        //
                        // Only update our local value if we are successful in inserting the value.
                        //
                        // TODO avoid clone
                        node.values[child_idx] += &Bucket::from(kv.value.clone());
                    }
                    ret
                }
            }
            Node::Leaf(node) => {
                // Returns the index of the first key that is greater than kv.key
                let key_idx = node.keys.partition_point(|key| key <= &kv.key);
                debug_assert!(
                    key_idx < node.keys.len(),
                    "should not get insert request for key outside bounds of leaf: key={} min={} max={}",
                    &kv.key,
                    node.keys[0],
                    node.keys[node.keys.len() - 1],
                );
                let bucket_idx = key_idx - 1;
                let bucket = &mut node.buckets[bucket_idx];
                if bucket.count >= self.bucket_size {
                    // Bucket is full, we need to create a new bucket.
                    // Need to split a bucket which means querying storage for the splits of the
                    // range and the hashes.
                    let bucket_lower_bound = &node.keys[key_idx - 1];
                    let bucket_upper_bound = &node.keys[key_idx];
                    let split = self
                        .store
                        //TODO: make this split_at and provide a count, all callers already have a
                        //count of values in the range so we can approximate the split by dividing
                        //the count.
                        .split(&bucket_lower_bound..&bucket_upper_bound)
                        .await?;
                    debug!(
                        ?node.keys,
                        ?node.buckets,
                        ?key_idx,
                        ?bucket_idx,
                        ?split,
                        ?bucket_lower_bound,
                        ?bucket_upper_bound,
                        "split bucket");

                    // Replace the split bucket with the new hashes.
                    node.buckets.splice(
                        bucket_idx..bucket_idx + 1,
                        split.hashes.into_iter().map(|hc| hc.into()),
                    );

                    // Insert the new keys, leaving existing keys untouched.
                    node.keys.splice(key_idx..key_idx, split.keys.into_iter());

                    // Note: The key has already been inserted into the store so we do not need to
                    // update the new buckets with the entry.
                } else {
                    // TODO avoid clone
                    *bucket += &Bucket::from(kv.value.clone());
                }
                Ok(())
            }
        }
    }

    fn is_node_full(&self, node: &Node<K, V>) -> bool {
        match node {
            Node::Internal(Internal { children, .. }) => children.len() >= self.size(),
            Node::Leaf(Leaf { buckets, .. }) => buckets.len() >= self.size(),
        }
    }

    async fn reduce(&mut self, range: Range<&K>) -> Result<Bucket<V>> {
        debug!(?range, "reduce");
        if range.start >= range.end {
            Ok(Bucket::default())
        } else if let Some(root) = self.root.take() {
            let ret = self.reduce_node(&root, range).await;
            self.root = Some(root);
            Ok(ret?.1)
        } else {
            Ok(Bucket::default())
        }
    }

    #[async_recursion]
    async fn reduce_node<'a>(
        &'a mut self,
        node: &Node<K, V>,
        search: Range<&'a K>,
    ) -> Result<(ReduceControl, Bucket<V>)> {
        match node {
            Node::Internal(node) => {
                // Returns the index of the first key that is greater than search.start.
                let start_key_idx = node.keys.partition_point(|key| key <= &search.start);
                debug_assert!(
                    0 < start_key_idx,
                    "should not get reduce request for key outside bounds of internal node"
                );
                // Translate the key index into an index into the children vector.
                let start_child_idx = start_key_idx - 1;
                let child = &node.children[start_child_idx];
                let (ctl, mut reduction) = self.reduce_node(&child, search.clone()).await?;
                match ctl {
                    ReduceControl::Exclude => Ok((ReduceControl::Exclude, reduction)),
                    ReduceControl::Include => {
                        // Returns the index of the first key that is greater than search.end.
                        let end_key_idx = node.keys.partition_point(|key| key <= &search.end);
                        // Translate the key index into an index into the children vector.
                        let end_child_idx = if end_key_idx == node.keys.len() {
                            // The search range goes past the end of this node, the end child is
                            // the last child.
                            node.children.len() - 1
                        } else {
                            end_key_idx - 1
                        };
                        for child in &node.children[start_child_idx + 1..=end_child_idx] {
                            reduction += &self.reduce_node_include(&child, &search.end).await?;
                        }
                        if &node.keys[node.keys.len() - 1] > search.end {
                            // We span the entire search range, we no longer need to
                            // search for siblings nodes.
                            Ok((ReduceControl::Exclude, reduction))
                        } else {
                            Ok((ReduceControl::Include, reduction))
                        }
                    }
                }
            }
            Node::Leaf(node) => self.reduce_leaf(node, search).await,
        }
    }
    // Reduce all values in this node and child nodes up to end
    #[async_recursion]
    async fn reduce_node_include(&mut self, node: &Node<K, V>, end: &K) -> Result<Bucket<V>> {
        match node {
            Node::Internal(node) => {
                let mut reduction = Bucket::default();
                // Returns the index of the first key that is greater than end
                let idx = node.keys.partition_point(|key| key <= end);
                // Translate the key index into an index into the children vector.
                let child_idx = if idx == node.keys.len() {
                    // The end goes past this node so the target child is the last child
                    node.children.len() - 1
                } else {
                    idx - 1
                };
                // Do not recurse into children that are fully covered by the range
                for i in 0..child_idx {
                    reduction += &node.values[i];
                }
                // Recurse into last matching child as it may be only a partial match
                let child = &node.children[child_idx];
                Ok(reduction + self.reduce_node_include(child, end).await?)
            }
            Node::Leaf(node) => Ok(self.reduce_leaf(node, &node.keys[0]..end).await?.1),
        }
    }

    async fn reduce_leaf(
        &mut self,
        node: &Leaf<K, V>,
        search: Range<&K>,
    ) -> Result<(ReduceControl, Bucket<V>)> {
        // Returns the index of the first key that is greater than search.start.
        let start_key_idx = node.keys.partition_point(|key| key <= search.start);
        debug_assert!(
            0 < start_key_idx && start_key_idx < node.keys.len(),
            "should not get reduce request for key outside bounds of leaf"
        );

        // Returns the index of the first key that is greater than search.end.
        let end_key_idx = node.keys.partition_point(|key| key <= search.end);
        // Based on the tree structure we are guaranteed that search.end
        //  * is greater than the minimum key of the node
        //  * is greater or equal to the start key index
        //
        //  However end_key_idx may be equal to keys.len() meaning the range goes past the
        //  end of this node.
        debug_assert!(
            0 < end_key_idx && start_key_idx <= end_key_idx,
            "should not get reduce request where search end is before start or minumum bound of node"
        );

        // Translate the key indexes into bucket indexes
        // start_bucket_idx is the index of the first bucket that intersects with the search range
        let start_bucket_idx = start_key_idx - 1;
        // end_bucket_idx is the index of the last bucket that intersects with the search range
        let end_bucket_idx = if end_key_idx == node.keys.len() {
            // Search range goes past the end of this node, end bucket is the last bucket
            node.buckets.len() - 1
        } else {
            end_key_idx - 1
        };

        // Check if the search range is inside a single bucket and does not extend past this node.
        if start_bucket_idx == end_bucket_idx && search.end <= &node.keys[start_key_idx] {
            // Search range must be inside the bucket bounds
            debug_assert!(
                search.start >= &node.keys[start_key_idx - 1],
                "search range starts before bucket: {} < {}",
                search.start,
                &node.keys[start_key_idx - 1]
            );
            // The search range is completely within a single bucket, make a single db
            // query and return.
            return Ok((
                ReduceControl::Exclude,
                self.store
                    .hash_range(search.start..search.end)
                    .await?
                    .into(),
            ));
        }

        // Index of the first bucket that is completely enclosed within the search range.
        let first_complete_bucket = if search.start == &node.keys[start_key_idx - 1] {
            // The search range starts exactly on the start of the bucket so the entire
            // bucket is enclosed within the search range.
            start_bucket_idx
        } else {
            start_bucket_idx + 1
        };

        // Index of the last bucket that is completely enclosed within the search range.
        let last_complete_bucket =
            if end_key_idx == node.keys.len() || &node.keys[end_key_idx] < search.end {
                // The search extends past the end of this bucket so the end_bucket_idx is the
                // last complete bucket
                end_bucket_idx
            } else {
                // The search range ends before the leaf range ends, therefore the last
                // complete bucket is the previous bucket
                end_bucket_idx - 1
            };

        let mut reduction: Bucket<V> = if first_complete_bucket != start_bucket_idx {
            // We have a partial starting bucket, compute the reduction
            let red = self
                .store
                .hash_range(search.start..&node.keys[start_key_idx])
                .await?
                .into();
            debug!(?red, "left hand reduction");
            red
        } else {
            Bucket::default()
        };

        // Include buckets that are fully spanned by the search range
        if first_complete_bucket <= last_complete_bucket {
            for bucket in &node.buckets[first_complete_bucket..=last_complete_bucket] {
                debug!(?bucket, "including bucket");
                reduction += bucket;
            }
        }

        if last_complete_bucket != end_bucket_idx {
            // We have a partial ending bucket, compute the reduction
            let red: Bucket<V> = self
                .store
                .hash_range(&node.keys[end_key_idx - 1]..search.end)
                .await?
                .into();
            debug!(?red, "right hand reduction");
            reduction += &red;

            // All data in the search range was contained in this leaf, no need to include
            // other nodes.
            Ok((ReduceControl::Exclude, reduction))
        } else {
            // Search range spans adjacent Leaf nodes include their values
            Ok((ReduceControl::Include, reduction))
        }
    }

    async fn insert(&mut self, item: &ReconItem<'_, K>) -> Result<bool> {
        let kv = Entry {
            value: V::from(item.key),
            // TODO avoid clone
            key: item.key.clone(),
        };
        // Insert into the store first,
        if self.store.insert(&ReconItem::new_key(&kv.key)).await? {
            // update the RedTree if its a new value
            self.insert_hash(&kv).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn insert_many(&self, items: &[ReconItem<'_, K>]) -> Result<InsertResult> {
        let mut insert_result = InsertResult::new(Vec::with_capacity(items.len()), 0);
        // TODO make this more efficient
        for item in items {
            insert_result.keys.push(self.insert(item).await?);
            insert_result.value_count += 1;
        }
        Ok(insert_result)
    }

    async fn hash_range(&mut self, range: Range<&K>) -> Result<HashCount<V>> {
        Ok(self.reduce(range).await?.into())
    }

    async fn range(
        &mut self,
        range: Range<&K>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        self.store.range(range, offset, limit).await
    }

    async fn range_with_values(
        &self,
        range: Range<&K>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (K, Vec<u8>)> + Send + 'static>> {
        self.store.range_with_values(range, offset, limit).await
    }

    //    async fn split(&self, range: Range<&K>) -> Result<Split<K, V>> {
    //        // TODO need to leverage the tree splits to produce splits
    //        if let Some(middle) = self.store.middle(range.clone()).await? {
    //            let left = self.hash_range(range.start..&middle).await?;
    //            let right = self.hash_range(&middle..range.end).await?;
    //            Ok(Split {
    //                keys: vec![middle],
    //                hashes: vec![left, right],
    //            })
    //        } else {
    //            // No keys in range return empty split
    //            Ok(Split {
    //                keys: vec![],
    //                hashes: vec![HashCount::new(Self::Hash::identity(), 0)],
    //            })
    //        }
    //    }

    async fn value_for_key(&self, key: &K) -> Result<Option<Vec<u8>>> {
        self.store.value_for_key(key).await
    }

    /// Report all keys in the range that are missing a value.
    async fn keys_with_missing_values(&self, range: Range<&K>) -> Result<Vec<K>> {
        self.store.keys_with_missing_values(range).await
    }
}

#[async_trait]
impl<K, V, S> recon::Store for RedTree<K, V, S>
where
    S: recon::Store<Key = K, Hash = V> + Send + Sync,
    K: recon::Key,
    V: recon::AssociativeHash,
    V: for<'a> From<&'a K>,
    V: for<'a> AddAssign<&'a V>,
    V: AddAssign<V>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default + Clone,
{
    type Key = K;

    type Hash = V;

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        self.inner.write().await.insert(item).await
    }

    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> Result<InsertResult> {
        self.inner.write().await.insert_many(items).await
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> Result<HashCount<Self::Hash>> {
        self.inner.write().await.hash_range(range).await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.inner.write().await.range(range, offset, limit).await
    }

    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.inner
            .read()
            .await
            .range_with_values(range, offset, limit)
            .await
    }

    async fn count(&self, range: Range<&Self::Key>) -> Result<usize> {
        // My guess is the tree is faster for computing the count even though it also computes the
        // hash along the way
        Ok(self.inner.write().await.hash_range(range).await?.count() as usize)
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.inner.read().await.value_for_key(key).await
    }

    /// Report all keys in the range that are missing a value.
    async fn keys_with_missing_values(&self, range: Range<&Self::Key>) -> Result<Vec<Self::Key>> {
        self.inner
            .read()
            .await
            .keys_with_missing_values(range)
            .await
    }
}

// Signal to tree traversal for reduce if we should include child reduction values or go back up the
// tree.
enum ReduceControl {
    Exclude,
    Include,
}

// Node within the tree.
#[derive(Debug)]
enum Node<K, V> {
    Internal(Internal<K, V>),
    Leaf(Leaf<K, V>),
}

impl<K, V> Node<K, V>
where
    K: recon::Key,
    V: for<'a> From<&'a K>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default,
    V: Clone,
{
    /// Split creates a sibling node from a given node by splitting the node in two around a median.
    /// split will split the child at b leaving the [0, b-1] keys
    /// while moving the set of [b, 2b-1] keys to the sibling.
    fn split(&mut self, b: usize) -> (K, Self) {
        match self {
            Node::Internal(Internal {
                keys,
                children,
                values,
            }) => {
                // Split keys into self and sibling
                let sibling_keys = keys.split_off(b);
                // Get copy of the median key, we leave the duplicate key on both leaves.
                let median_key = sibling_keys[0].clone();
                keys.push(median_key.clone());

                // Split the children into self and sibling
                let sibling_children = children.split_off(b);
                // Split the values into self and sibling
                let sibling_values = values.split_off(b);

                debug_assert_eq!(keys.len(), children.len() + 1);
                debug_assert_eq!(children.len(), values.len());

                debug_assert_eq!(sibling_keys.len(), sibling_children.len() + 1);
                debug_assert_eq!(sibling_children.len(), sibling_values.len());
                (
                    median_key,
                    Node::Internal(Internal {
                        keys: sibling_keys,
                        children: sibling_children,
                        values: sibling_values,
                    }),
                )
            }

            Node::Leaf(Leaf { keys, buckets }) => {
                // Split keys into self and sibling
                let sibling_keys = keys.split_off(b);

                // Get copy of the median key, we leave the duplicate key on both leaves.
                let median_key = sibling_keys[0].clone();
                keys.push(median_key.clone());

                // Split the buckets into self and sibling
                let sibling_buckets = buckets.split_off(b);

                debug_assert_eq!(keys.len(), buckets.len() + 1);
                debug_assert_eq!(sibling_keys.len(), sibling_buckets.len() + 1);
                (
                    median_key,
                    Node::Leaf(Leaf {
                        keys: sibling_keys,
                        buckets: sibling_buckets,
                    }),
                )
            }
        }
    }

    fn reduce(&self) -> Bucket<V> {
        match self {
            Node::Internal(Internal { values, .. })
            | Node::Leaf(Leaf {
                buckets: values, ..
            }) => values
                .iter()
                .fold(Bucket::default(), |acc, bucket| acc + bucket),
        }
    }
}

struct Internal<K, V> {
    // Keys that bound the children, keys.len() == children.len() + 1 always
    // Children are bounded by the surrounding keys.
    keys: Vec<K>,
    // List of children of the node
    children: Vec<Node<K, V>>,
    // Reduced values for each child
    values: Vec<Bucket<V>>,
}

impl<K, V> std::fmt::Debug for Internal<K, V>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Internal")
            .field(
                "keys",
                &self
                    .keys
                    .iter()
                    .map(|b| format!("{b:?}"))
                    .collect::<Vec<String>>(),
            )
            .field("children", &self.children)
            .field(
                "values",
                &self
                    .values
                    .iter()
                    .map(|b| format!("{b:?}"))
                    .collect::<Vec<String>>(),
            )
            .finish()
    }
}
struct Leaf<K, V> {
    // Keys that bound buckets, keys.len() == buckets.len() + 1 always
    // Buckets are bounded by the surrounding keys.
    // The first and last keys are duplicated in the tree
    keys: Vec<K>,
    // Reduced values for each child
    buckets: Vec<Bucket<V>>,
}

impl<K, V> Leaf<K, V>
where
    V: Default,
{
    fn new(b: usize, bounds: (K, K)) -> Self {
        let size = 2 * b - 1;
        let mut keys = Vec::with_capacity(size);
        keys.push(bounds.0);
        keys.push(bounds.1);
        let mut buckets = Vec::with_capacity(size);
        buckets.push(Bucket::default());
        Leaf { keys, buckets }
    }
}

impl<K, V> std::fmt::Debug for Leaf<K, V>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leaf")
            .field(
                "keys",
                &self
                    .keys
                    .iter()
                    .map(|b| format!("{b:?}"))
                    .collect::<Vec<String>>(),
            )
            .field(
                "buckets",
                &self
                    .buckets
                    .iter()
                    .map(|b| format!("{b:?}"))
                    .collect::<Vec<String>>(),
            )
            .finish()
    }
}

// TODO should we just use HashCount directly?
#[derive(Clone, Debug)]
struct Bucket<V> {
    reduction: V,
    count: u64,
}
impl<V> From<Bucket<V>> for HashCount<V>
where
    V: Clone,
{
    fn from(value: Bucket<V>) -> Self {
        Self::new(value.reduction, value.count)
    }
}
impl<V> From<HashCount<V>> for Bucket<V>
where
    V: Clone,
{
    fn from(value: HashCount<V>) -> Self {
        let (reduction, count) = value.into_inner();
        Self { reduction, count }
    }
}
impl<V> From<V> for Bucket<V> {
    fn from(value: V) -> Self {
        Self {
            reduction: value,
            count: 1,
        }
    }
}
impl<V> Default for Bucket<V>
where
    V: Default,
{
    fn default() -> Self {
        Self {
            reduction: V::default(),
            count: 0,
        }
    }
}

impl<'a, V> Add<&'a Bucket<V>> for Bucket<V>
where
    V: Add<&'a V, Output = V>,
{
    type Output = Self;

    fn add(self, rhs: &'a Bucket<V>) -> Self::Output {
        Self {
            reduction: self.reduction + &rhs.reduction,
            count: self.count + rhs.count,
        }
    }
}
impl<V> Add<Bucket<V>> for Bucket<V>
where
    V: Add<V, Output = V>,
{
    type Output = Self;

    fn add(self, rhs: Bucket<V>) -> Self::Output {
        Self {
            reduction: self.reduction + rhs.reduction,
            count: self.count + rhs.count,
        }
    }
}
impl<V> AddAssign<Bucket<V>> for Bucket<V>
where
    V: AddAssign<V>,
{
    fn add_assign(&mut self, rhs: Bucket<V>) {
        self.reduction += rhs.reduction;
        self.count += rhs.count;
    }
}

impl<V> AddAssign<&Bucket<V>> for Bucket<V>
where
    V: for<'a> AddAssign<&'a V>,
{
    fn add_assign(&mut self, rhs: &Bucket<V>) {
        self.reduction += &rhs.reduction;
        self.count += rhs.count;
    }
}

#[async_trait]
impl<S> iroh_bitswap::Store for RedTree<EventId, Sha256a, S>
where
    S: iroh_bitswap::Store,
{
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        self.inner.read().await.store.get_size(cid).await
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        self.inner.read().await.store.get(cid).await
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        self.inner.read().await.store.has(cid).await
    }
}
#[async_trait]
impl<S> ceramic_api::AccessModelStore for RedTree<EventId, Sha256a, S>
where
    S: ceramic_api::AccessModelStore,
{
    /// Returns (new_key, new_value) where true if was newly inserted, false if it already existed.
    async fn insert(&self, key: EventId, value: Option<Vec<u8>>) -> Result<(bool, bool)> {
        self.inner.write().await.store.insert(key, value).await
    }
    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(EventId, Vec<u8>)>> {
        self.inner
            .read()
            .await
            .store
            .range_with_values(start, end, offset, limit)
            .await
    }

    async fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        self.inner.read().await.store.value_for_key(key).await
    }

    // it's likely `highwater` will be a string or struct when we have alternative storage for now we
    // keep it simple to allow easier error propagation. This isn't currently public outside of this repo.
    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<EventId>)> {
        self.inner
            .read()
            .await
            .store
            .keys_since_highwater_mark(highwater, limit)
            .await
    }
}

#[cfg(test)]
mod tests {

    use std::{array::TryFromSliceError, collections::BTreeSet};

    use expect_test::expect;
    use proptest::prelude::*;
    use recon::{AssociativeHash, Key, Store};
    use test_log::test;

    use super::*;

    #[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
    struct TestKey([u8; 4]);

    impl std::fmt::Debug for TestKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_tuple("TestKey").field(&self.as_u32()).finish()
        }
    }

    impl TestKey {
        fn new(k: u32) -> Self {
            Self(k.to_be_bytes())
        }
        fn as_u32(&self) -> u32 {
            u32::from_be_bytes(self.0)
        }
    }

    impl std::fmt::Display for TestKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "k{}", self.as_u32())
        }
    }

    impl TryFrom<Vec<u8>> for TestKey {
        type Error = TryFromSliceError;

        fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
            Ok(TestKey(value.as_slice().try_into()?))
        }
    }

    impl recon::Key for TestKey {
        fn min_value() -> Self {
            TestKey::new(u32::MIN)
        }

        fn max_value() -> Self {
            TestKey::new(u32::MAX)
        }

        fn as_bytes(&self) -> &[u8] {
            &self.0[..]
        }

        fn is_fencepost(&self) -> bool {
            self == &Self::min_value() || self == &Self::max_value()
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Default)]
    struct TestValue(u32);

    impl<'a> From<&'a TestKey> for TestValue {
        fn from(value: &'a TestKey) -> Self {
            Self(value.as_u32())
        }
    }

    impl std::fmt::Display for TestValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "v{}", self.0)
        }
    }

    impl From<[u32; 8]> for TestValue {
        fn from(value: [u32; 8]) -> Self {
            // HACK
            Self(value[0])
        }
    }

    impl Add for TestValue {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(self.0 + rhs.0)
        }
    }
    impl<'a> Add<&'a TestValue> for TestValue {
        type Output = Self;

        fn add(self, rhs: &'a TestValue) -> Self::Output {
            Self(self.0 + rhs.0)
        }
    }
    impl AddAssign<TestValue> for TestValue {
        fn add_assign(&mut self, rhs: TestValue) {
            self.0 += rhs.0;
        }
    }
    impl AddAssign<&TestValue> for TestValue {
        fn add_assign(&mut self, rhs: &TestValue) {
            self.0 += rhs.0;
        }
    }

    impl AssociativeHash for TestValue {
        fn digest<K: Key>(key: &K) -> Self {
            let bytes = key.as_bytes();
            Self(u32::from_be_bytes(bytes.try_into().unwrap()))
        }

        fn as_bytes(&self) -> [u8; 32] {
            todo!()
        }

        fn as_u32s(&self) -> &[u32; 8] {
            todo!()
        }
    }

    #[test(tokio::test)]
    async fn offline_build_tree() -> Result<()> {
        let keys = [
            10, 65, 42, 88, 62, 98, 91, 16, 37, 19, 45, 54, 85, 79, 46, 77, 12,
        ];
        let store = recon::BTreeStore::default();
        for i in keys {
            store.insert(&ReconItem::new_key(&TestKey::new(i))).await?;
        }

        let tree = build_tree_with_store(2, 4, store).await;
        // This is a simple test to observe that the tree is balanced and that the reduction values
        // are correct.
        expect![[r#"
            RedTreeInner {
                root: Some(
                    Internal(
                        Internal {
                            keys: [
                                "TestKey(0)",
                                "TestKey(54)",
                                "TestKey(4294967295)",
                            ],
                            children: [
                                Internal(
                                    Internal {
                                        keys: [
                                            "TestKey(0)",
                                            "TestKey(37)",
                                            "TestKey(54)",
                                        ],
                                        children: [
                                            Leaf(
                                                Leaf {
                                                    keys: [
                                                        "TestKey(0)",
                                                        "TestKey(16)",
                                                        "TestKey(37)",
                                                    ],
                                                    buckets: [
                                                        "Bucket { reduction: TestValue(22), count: 2 }",
                                                        "Bucket { reduction: TestValue(35), count: 2 }",
                                                    ],
                                                },
                                            ),
                                            Leaf(
                                                Leaf {
                                                    keys: [
                                                        "TestKey(37)",
                                                        "TestKey(45)",
                                                        "TestKey(54)",
                                                    ],
                                                    buckets: [
                                                        "Bucket { reduction: TestValue(79), count: 2 }",
                                                        "Bucket { reduction: TestValue(91), count: 2 }",
                                                    ],
                                                },
                                            ),
                                        ],
                                        values: [
                                            "Bucket { reduction: TestValue(57), count: 4 }",
                                            "Bucket { reduction: TestValue(170), count: 4 }",
                                        ],
                                    },
                                ),
                                Internal(
                                    Internal {
                                        keys: [
                                            "TestKey(54)",
                                            "TestKey(79)",
                                            "TestKey(4294967295)",
                                        ],
                                        children: [
                                            Leaf(
                                                Leaf {
                                                    keys: [
                                                        "TestKey(54)",
                                                        "TestKey(65)",
                                                        "TestKey(79)",
                                                    ],
                                                    buckets: [
                                                        "Bucket { reduction: TestValue(116), count: 2 }",
                                                        "Bucket { reduction: TestValue(142), count: 2 }",
                                                    ],
                                                },
                                            ),
                                            Leaf(
                                                Leaf {
                                                    keys: [
                                                        "TestKey(79)",
                                                        "TestKey(88)",
                                                        "TestKey(4294967295)",
                                                    ],
                                                    buckets: [
                                                        "Bucket { reduction: TestValue(164), count: 2 }",
                                                        "Bucket { reduction: TestValue(277), count: 3 }",
                                                    ],
                                                },
                                            ),
                                        ],
                                        values: [
                                            "Bucket { reduction: TestValue(258), count: 4 }",
                                            "Bucket { reduction: TestValue(441), count: 5 }",
                                        ],
                                    },
                                ),
                            ],
                            values: [
                                "Bucket { reduction: TestValue(227), count: 8 }",
                                "Bucket { reduction: TestValue(699), count: 9 }",
                            ],
                        },
                    ),
                ),
                b: 2,
                bucket_size: 4,
                store: "<>",
            }
        "#]]
        .assert_debug_eq(&tree);
        Ok(())
    }

    #[test(tokio::test)]
    async fn online_build_tree() -> Result<()> {
        let keys = [
            10, 65, 42, 88, 62, 98, 91, 16, 37, 19, 45, 54, 85, 79, 46, 77, 12,
        ];
        let mut tree = build_tree(2, 4).await;
        for i in keys {
            tree.insert(&ReconItem::new_key(&TestKey::new(i))).await?;
        }
        // This is a simple test to observe that the tree is balanced and that the reduction values
        // are correct.
        expect![[r#"
            RedTreeInner {
                root: Some(
                    Internal(
                        Internal {
                            keys: [
                                "TestKey(0)",
                                "TestKey(88)",
                                "TestKey(4294967295)",
                            ],
                            children: [
                                Internal(
                                    Internal {
                                        keys: [
                                            "TestKey(0)",
                                            "TestKey(62)",
                                            "TestKey(88)",
                                        ],
                                        children: [
                                            Internal(
                                                Internal {
                                                    keys: [
                                                        "TestKey(0)",
                                                        "TestKey(42)",
                                                        "TestKey(62)",
                                                    ],
                                                    children: [
                                                        Leaf(
                                                            Leaf {
                                                                keys: [
                                                                    "TestKey(0)",
                                                                    "TestKey(19)",
                                                                    "TestKey(42)",
                                                                ],
                                                                buckets: [
                                                                    "Bucket { reduction: TestValue(38), count: 3 }",
                                                                    "Bucket { reduction: TestValue(56), count: 2 }",
                                                                ],
                                                            },
                                                        ),
                                                        Leaf(
                                                            Leaf {
                                                                keys: [
                                                                    "TestKey(42)",
                                                                    "TestKey(62)",
                                                                ],
                                                                buckets: [
                                                                    "Bucket { reduction: TestValue(187), count: 4 }",
                                                                ],
                                                            },
                                                        ),
                                                    ],
                                                    values: [
                                                        "Bucket { reduction: TestValue(94), count: 5 }",
                                                        "Bucket { reduction: TestValue(187), count: 4 }",
                                                    ],
                                                },
                                            ),
                                            Internal(
                                                Internal {
                                                    keys: [
                                                        "TestKey(62)",
                                                        "TestKey(88)",
                                                    ],
                                                    children: [
                                                        Leaf(
                                                            Leaf {
                                                                keys: [
                                                                    "TestKey(62)",
                                                                    "TestKey(77)",
                                                                    "TestKey(88)",
                                                                ],
                                                                buckets: [
                                                                    "Bucket { reduction: TestValue(127), count: 2 }",
                                                                    "Bucket { reduction: TestValue(241), count: 3 }",
                                                                ],
                                                            },
                                                        ),
                                                    ],
                                                    values: [
                                                        "Bucket { reduction: TestValue(368), count: 5 }",
                                                    ],
                                                },
                                            ),
                                        ],
                                        values: [
                                            "Bucket { reduction: TestValue(281), count: 9 }",
                                            "Bucket { reduction: TestValue(368), count: 5 }",
                                        ],
                                    },
                                ),
                                Internal(
                                    Internal {
                                        keys: [
                                            "TestKey(88)",
                                            "TestKey(4294967295)",
                                        ],
                                        children: [
                                            Internal(
                                                Internal {
                                                    keys: [
                                                        "TestKey(88)",
                                                        "TestKey(4294967295)",
                                                    ],
                                                    children: [
                                                        Leaf(
                                                            Leaf {
                                                                keys: [
                                                                    "TestKey(88)",
                                                                    "TestKey(4294967295)",
                                                                ],
                                                                buckets: [
                                                                    "Bucket { reduction: TestValue(277), count: 3 }",
                                                                ],
                                                            },
                                                        ),
                                                    ],
                                                    values: [
                                                        "Bucket { reduction: TestValue(277), count: 3 }",
                                                    ],
                                                },
                                            ),
                                        ],
                                        values: [
                                            "Bucket { reduction: TestValue(277), count: 3 }",
                                        ],
                                    },
                                ),
                            ],
                            values: [
                                "Bucket { reduction: TestValue(649), count: 14 }",
                                "Bucket { reduction: TestValue(277), count: 3 }",
                            ],
                        },
                    ),
                ),
                b: 2,
                bucket_size: 4,
                store: "<>",
            }
        "#]].assert_debug_eq(&tree);
        Ok(())
    }

    #[test(tokio::test)]
    async fn simple_offline_insert_reduce() {
        let b = 2;
        let bucket_size = 2;
        let keys = vec![
            40354, 12607, 4655, 4660, 48119, 43419, 40338, 24822, 2944, 65228, 2524, 39824, 15219,
            5055, 8323,
        ];
        let ranges = vec![(0, 7)];
        do_offline_insert_reduce(b, bucket_size, (keys, ranges)).await;
    }

    #[test(tokio::test)]
    async fn simple_insert_reduce() {
        let b = 2;
        let bucket_size = 2;
        let keys = vec![1, 2, 3, 4, 5];
        let ranges = vec![
            (0, 1),
            (0, 2),
            (0, 3),
            (0, 4),
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 3),
            (2, 4),
            (3, 4),
        ];
        do_online_insert_reduce(b, bucket_size, (keys.clone(), ranges.clone())).await;
        do_offline_insert_reduce(b, bucket_size, (keys, ranges)).await;
    }
    #[test(tokio::test)]
    async fn duplicates_insert_reduce() {
        let b = 2;
        let bucket_size = 2;
        let keys = vec![1, 1, 2, 2, 3, 4, 4, 5, 5];
        let ranges = vec![
            (0, 1),
            (0, 2),
            (0, 3),
            (0, 4),
            (0, 5),
            (0, 6),
            (0, 7),
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
            (1, 6),
            (1, 7),
            (2, 3),
            (2, 4),
            (2, 5),
            (2, 6),
            (2, 7),
            (3, 4),
            (3, 5),
            (3, 6),
            (3, 7),
            (4, 5),
            (4, 6),
            (4, 7),
            (5, 6),
            (5, 7),
            (6, 7),
        ];
        do_online_insert_reduce(b, bucket_size, (keys.clone(), ranges.clone())).await;
        do_offline_insert_reduce(b, bucket_size, (keys, ranges)).await;
    }

    async fn build_tree(
        b: usize,
        bucket_size: u64,
    ) -> RedTreeInner<TestKey, TestValue, recon::BTreeStore<TestKey, TestValue>> {
        build_tree_with_store(b, bucket_size, recon::BTreeStore::default()).await
    }
    async fn build_tree_with_store(
        b: usize,
        bucket_size: u64,
        store: recon::BTreeStore<TestKey, TestValue>,
    ) -> RedTreeInner<TestKey, TestValue, recon::BTreeStore<TestKey, TestValue>> {
        RedTreeInner::builder()
            .with_b(b)
            .with_bucket_size(bucket_size)
            .build(store)
            .await
            .unwrap()
    }

    // Insert data into RedTree directly
    async fn do_online_insert_reduce(
        b: usize,
        bucket_size: u64,
        (keys, ranges): (Vec<u16>, Vec<(usize, usize)>),
    ) {
        let mut tree = build_tree(b, bucket_size).await;
        // Insert keys in the order they exist
        for i in &keys {
            tree.insert(&ReconItem::new_key(&TestKey::new(*i as u32)))
                .await
                .unwrap();
        }
        do_reduce(tree, (keys, ranges)).await;
    }

    // Load data into store first then create the RedTree
    async fn do_offline_insert_reduce(
        b: usize,
        bucket_size: u64,
        (keys, ranges): (Vec<u16>, Vec<(usize, usize)>),
    ) {
        let store = recon::BTreeStore::default();
        // Insert keys in the order they exist
        for i in &keys {
            store
                .insert(&ReconItem::new_key(&TestKey::new(*i as u32)))
                .await
                .unwrap();
        }
        let tree = build_tree_with_store(b, bucket_size, store).await;
        do_reduce(tree, (keys, ranges)).await;
    }

    // Reduces the keys directly and the tree to ensure they get the same result
    async fn do_reduce(
        mut tree: RedTreeInner<TestKey, TestValue, recon::BTreeStore<TestKey, TestValue>>,
        (mut keys, ranges): (Vec<u16>, Vec<(usize, usize)>),
    ) {
        // Sort the keys so the ranges are a meaningful range.
        keys.sort();
        println!("KEYS: {keys:#?}\nTREE {tree:#?}");

        // Remove duplicates from the keys
        let keys_set = BTreeSet::<u32>::from_iter(keys.iter().map(|key| *key as u32));

        for range in ranges {
            let start = keys[range.0] as u32;
            let end = keys[range.1] as u32;
            if start == end {
                // This can happen if the generated input keys has duplicates
                continue;
            }
            let reduction = tree
                .reduce(&TestKey::new(start)..&TestKey::new(end))
                .await
                .unwrap();
            println!(
                "Ranged Keys: {:?}",
                &keys_set.range(start..end).collect::<Vec<_>>()
            );
            assert_eq!(
                keys_set
                    .range(start..end)
                    .fold(0 as u32, |acc, v| acc + (*v as u32)),
                reduction.reduction.0,
                "redtree should reduce"
            );
            assert_eq!(
                keys_set.range(start..end).count() as u64,
                reduction.count,
                "redtree should count"
            );
        }
    }

    // Produce a set of keys and some ranges over those keys
    fn keys_and_ranges() -> impl Strategy<Value = (Vec<u16>, Vec<(usize, usize)>)> {
        // First choose the length of the keys vector,
        (2..500usize)
            // Generate keys. We do not generate keys that are equal to MIN or MAX.
            .prop_flat_map(|size| vec![InelasticValue(1..u16::MAX - 1); size])
            .prop_flat_map(|vec| {
                let len = vec.len();
                (
                    Just(vec),
                    prop::collection::vec(0..len - 1, 1..10).prop_flat_map(move |start| {
                        start
                            .into_iter()
                            .map(|start| (Just(start), start + 1..len))
                            .collect::<Vec<(Just<usize>, Range<usize>)>>()
                    }),
                )
            })
    }

    proptest! {
        #[test]
        fn online_insert_reduce(b in 2..10usize, bucket_size in 2..10u64, keys_and_ranges in keys_and_ranges()) {
            tokio::runtime::Runtime::new().unwrap().block_on(do_online_insert_reduce(b, bucket_size, keys_and_ranges))
        }
    }
    proptest! {
        #[test]
        fn offline_insert_reduce(b in 2..10usize, bucket_size in 2..10u64, keys_and_ranges in keys_and_ranges()) {
            tokio::runtime::Runtime::new().unwrap().block_on(do_offline_insert_reduce(b, bucket_size, keys_and_ranges))
        }
    }

    // Strategy that produces values that do not shrink.
    // This helps shrinking spend iterations shrinking meaningful aspects of the tests instead of
    // shrinking this value.
    #[derive(Debug, Clone)]
    struct InelasticValue<T>(Range<T>);
    struct InelasticValueTree<T>(T);

    impl<T: Copy + std::fmt::Debug> proptest::strategy::ValueTree for InelasticValueTree<T> {
        type Value = T;

        fn current(&self) -> Self::Value {
            self.0
        }

        fn simplify(&mut self) -> bool {
            false
        }

        fn complicate(&mut self) -> bool {
            false
        }
    }

    impl<T> Strategy for InelasticValue<T>
    where
        T: Copy + std::fmt::Debug + rand::distributions::uniform::SampleUniform,
        Range<T>: rand::distributions::uniform::SampleRange<T>,
    {
        type Tree = InelasticValueTree<T>;

        type Value = T;

        fn new_tree(
            &self,
            runner: &mut proptest::test_runner::TestRunner,
        ) -> proptest::strategy::NewTree<Self> {
            Ok(InelasticValueTree(runner.rng().gen_range(self.0.clone())))
        }
    }
}
