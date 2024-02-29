use std::ops::{Add, AddAssign, Range};

use anyhow::Result;
use async_recursion::async_recursion;
use recon::{HashCount, ReconItem};
use tracing::debug;

/// Red(uction)Tree is a variant of BTree that is only the top portion of a btree.
/// The tree is in memory and can serve queries about the large ranges of the tree but delegates to
/// another implementation for more fine grained ranges of the tree.
///
/// It is named a Reduction Tree because the only query supported is a reduction of values within a
/// range. The reduction of the value must be associative and commutative so that it can be incrementally reduced and
/// stored on intermediate nodes.
pub struct RedTree<K, V, S> {
    root: Option<Node<K, V>>,
    b: usize,
    bucket_size: usize,
    store: S,
}

impl<K, V, S> std::fmt::Debug for RedTree<K, V, S>
where
    K: std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedTree")
            .field("root", &self.root)
            .field("b", &self.b)
            .field("bucket_size", &self.bucket_size)
            .finish()
    }
}

pub struct Builder {
    b: usize,
    bucket_size: usize,
}

impl Builder {
    pub fn with_b(self, b: usize) -> Self {
        Self { b, ..self }
    }
    pub fn with_bucket_size(self, bucket_size: usize) -> Self {
        Self {
            bucket_size,
            ..self
        }
    }
    pub fn build<K, V, S>(self, store: S) -> RedTree<K, V, S>
    where
        S: recon::Store<Key = K, Hash = V>,
    {
        RedTree {
            root: None,
            store,
            b: self.b,
            bucket_size: self.bucket_size,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry<K, V> {
    key: K,
    value: V,
}

impl<K, V> Entry<K, V> {
    /// Construct a new entry
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K, V, S> RedTree<K, V, S> {
    /// Create a builder for constructing a RedTree.
    pub fn builder() -> Builder {
        Builder {
            b: 16,
            bucket_size: 2_usize.pow(15),
        }
    }
}
impl<K, V, S> RedTree<K, V, S>
where
    S: recon::Store<Key = K, Hash = V> + Send,
    K: recon::Key,
    V: recon::AssociativeHash,
    V: for<'a> From<&'a K>,
    V: for<'a> AddAssign<&'a V>,
    V: Add<V, Output = V>,
    V: for<'a> Add<&'a V, Output = V>,
    V: Default + Clone,
{
    /// Insert a ReconItem into the store
    pub async fn insert(&mut self, kv: Entry<K, V>) -> Result<bool> {
        // Insert into the store first,
        if self.store.insert(ReconItem::new_key(&kv.key)).await? {
            // update the RedTree if its a new value
            self.insert_hash(&kv).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Insert a key value pair possibly splitting nodes along the way.
    async fn insert_hash(&mut self, kv: &Entry<K, V>) -> Result<()> {
        debug!(?kv, "insert_hash");
        if let Some(mut root) = self.root.take() {
            if self.is_node_full(&root) {
                // split the root creating a new root and child nodes along the way.
                let (median, sibling) = root.split(self.b);
                let root_value = root.reduce();
                let sibling_value = sibling.reduce();
                let size = 2 * self.b - 1;
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
                // Translate the key index into an index into the children vector.
                let child_idx = key_idx - 1;
                let child = &mut node.children[child_idx];
                if self.is_node_full(&child) {
                    let (median, mut sibling) = child.split(self.b);

                    // Recurse
                    let ret = if kv.key <= median {
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
                        node.values[child_idx] += &kv.value;
                    }
                    ret
                }
            }
            Node::Leaf(node) => {
                // Returns the index of the first key that is greater than kv.key
                let key_idx = node.keys.partition_point(|key| key <= &kv.key);
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
                    println!(
                        "{:#?}",
                        self.store
                            .full_range()
                            .await?
                            .map(|event_id| format!("{event_id:?}"))
                            .collect::<Vec<String>>(),
                    );

                    // Replace the split bucket with the new hashes.
                    node.buckets.splice(
                        bucket_idx..bucket_idx + 1,
                        split.hashes.into_iter().map(|hc| Bucket {
                            // TODO add API to HashCount type to consumer inner without cloning
                            reduction: hc.hash().clone(),
                            count: hc.count() as usize,
                        }),
                    );

                    // Insert the new keys, leaving existing keys untouched.
                    node.keys.splice(key_idx..key_idx, split.keys.into_iter());

                    // Note: The key has already been inserted into the store so we do not need to
                    // update the new buckets with the entry.
                } else {
                    *bucket += &kv.value;
                }
                Ok(())
            }
        }
    }

    fn is_node_full(&self, node: &Node<K, V>) -> bool {
        match node {
            Node::Internal(Internal { children, .. }) => children.len() >= (2 * self.b - 1),
            Node::Leaf(Leaf { buckets, .. }) => buckets.len() >= (2 * self.b - 1),
        }
    }

    pub async fn reduce(&mut self, range: Range<&K>) -> Result<V> {
        debug!(?range, "reduce");
        if range.start >= range.end {
            Ok(V::default())
        } else if let Some(root) = self.root.take() {
            let ret = self.reduce_node(&root, range).await;
            self.root = Some(root);
            Ok(ret?.1)
        } else {
            Ok(V::default())
        }
    }

    #[async_recursion]
    async fn reduce_node<'a>(
        &'a mut self,
        node: &Node<K, V>,
        search: Range<&'a K>,
    ) -> Result<(ReduceControl, V)> {
        match node {
            Node::Internal(node) => {
                // Returns the index of the first key that is greater than search.start.
                let start_key_idx = node.keys.partition_point(|key| key <= &search.start);
                // Based on the tree structure we are guaranteed that search.start is greater than
                // or equal to the minimum key.
                debug_assert!(0 < start_key_idx);
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
    async fn reduce_node_include(&mut self, node: &Node<K, V>, end: &K) -> Result<V> {
        match node {
            Node::Internal(node) => {
                let mut reduction = V::default();
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
                    debug!("skipping recursion into child");
                    reduction += &node.values[i].reduction;
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
        range: Range<&K>,
    ) -> Result<(ReduceControl, V)> {
        // Returns the index of the first key that is greater than search.start.
        let start_key_idx = node.keys.partition_point(|key| key <= range.start);
        // Based on the tree structure we are guaranteed that search.start is not the greater than
        // or equal to the last key and that its greater than the first key
        debug_assert!(0 < start_key_idx && start_key_idx < node.keys.len());

        // Returns the index of the first key that is greater than search.end.
        let end_key_idx = node.keys.partition_point(|key| key <= range.end);
        // Based on the tree structure we are guaranteed that search.end
        //  * is greater than the minimum key of the node
        //  * is greater or equal to the start key index
        //
        //  However end_key_idx may be equal to keys.len() meaning the range goes past the
        //  end of this node.
        debug_assert!(0 < end_key_idx && start_key_idx <= end_key_idx);

        // Translate the key indexes into bucket indexes
        // start_bucket_idx is the index of the first bucket that intersects with the range
        let start_bucket_idx = start_key_idx - 1;
        // end_bucket_idx is the index of the last bucket that intersects with the range
        let end_bucket_idx = if end_key_idx == node.keys.len() {
            // Search range goes past the end of this node, end bucket is the last bucket
            node.buckets.len() - 1
        } else {
            end_key_idx - 1
        };

        // The lower bound of the starting bucket
        let lower_bucket_bound = &node.keys[start_key_idx - 1];
        // The upper bound of the ending bucket
        let upper_bucket_bound = if end_key_idx < node.keys.len() {
            &node.keys[end_key_idx]
        } else {
            &node.keys[node.keys.len() - 1]
        };

        debug!(?lower_bucket_bound, ?upper_bucket_bound, "bounds");
        if lower_bucket_bound <= range.start && range.end <= upper_bucket_bound {
            // The search range is completely within a single bucket, make a single db
            // query and return.
            return Ok((
                ReduceControl::Exclude,
                self.store
                    .hash_range(range.start..range.end)
                    .await?
                    .hash()
                    .clone(),
            ));
        }

        // Index of the first bucket that is completely enclosed within the search range.
        let first_complete_bucket = if range.start == lower_bucket_bound {
            // The search range starts exactly on the start of the bucket so the entire
            // bucket is enclosed within the search range.
            start_bucket_idx
        } else {
            start_bucket_idx + 1
        };

        // Index of the last bucket that is completely enclosed within the search range.
        // In case 1 this will be the last bucket of the leaf.
        // In case 2 this will be the last bucket that does not contain any keys outside the range.
        let last_complete_bucket =
            if end_key_idx == node.keys.len() || &node.keys[end_key_idx] < range.end {
                // The search extends past the end of this bucket so the end_bucket_idx is the
                // last complete bucket
                end_bucket_idx
            } else {
                // The search range ends before the end_bucket_idx range ends, therefore the last
                // complete bucket is the previous bucket
                end_bucket_idx - 1
            };

        let mut reduction = if first_complete_bucket != start_bucket_idx {
            // We have a partial starting bucket, compute the reduction
            let red = self
                .store
                .hash_range(range.start..&node.keys[start_key_idx])
                .await?
                .hash()
                // TODO avoid clone
                .clone();
            debug!(?red, "left hand reduction");
            red
        } else {
            V::default()
        };
        debug!(
            ?range,
            start_key_idx,
            start_bucket_idx,
            end_key_idx,
            end_bucket_idx,
            last_complete_bucket,
            ?reduction,
            "reduce leaf"
        );

        // Include buckets that are fully spanned by the range
        if first_complete_bucket <= last_complete_bucket {
            for bucket in &node.buckets[first_complete_bucket..=last_complete_bucket] {
                debug!(?bucket.reduction, "including bucket");
                reduction += &bucket.reduction;
            }
        }

        if last_complete_bucket != end_bucket_idx {
            // We have a partial ending bucket, compute the reduction
            let red = self
                .store
                .hash_range(&node.keys[end_key_idx - 1]..range.end)
                .await?;
            debug!(?red, "right hand reduction");
            reduction += red.hash();

            // All data in the range was contained in this leaf, no need to include
            // other nodes.
            Ok((ReduceControl::Exclude, reduction))
        } else {
            // Range spans adjacent Leaf nodes include their values
            Ok((ReduceControl::Include, reduction))
        }
    }
}

// Signal to tree traversal for reduce if we should include child reduction values or go back up the
// tree.
enum ReduceControl {
    Exclude,
    Include,
}

// Node within the tree.
//
// NOTE: Internal nodes and Leaf nodes differ in how many keys they store.
//
// Leaf nodes need to know the absolute bounds of the node as they have no children so they always
// have one _more_ key than buckets.
//
// Internal can delegate the outermost bounds to their children so they always have one _less_ key
// than children.
//
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

#[derive(Clone, Debug)]
struct Bucket<V> {
    reduction: V,
    count: usize,
}

impl<V> From<HashCount<V>> for Bucket<V>
where
    V: Clone,
{
    fn from(value: HashCount<V>) -> Self {
        Self {
            reduction: value.hash().clone(),
            count: value.count() as usize,
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

impl<V> From<V> for Bucket<V> {
    fn from(value: V) -> Self {
        Self {
            reduction: value,
            count: 1,
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
impl<V> Add<V> for Bucket<V>
where
    V: Add<V, Output = V>,
{
    type Output = Self;

    fn add(self, rhs: V) -> Self::Output {
        Self {
            reduction: self.reduction + rhs,
            count: self.count + 1,
        }
    }
}
impl<V> AddAssign<&V> for Bucket<V>
where
    V: for<'a> AddAssign<&'a V>,
{
    fn add_assign(&mut self, rhs: &V) {
        self.reduction += rhs;
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {

    use std::{array::TryFromSliceError, collections::BTreeSet};

    use expect_test::expect;
    use proptest::prelude::*;
    use recon::{AssociativeHash, Key};
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
    async fn insert_bucket_split() -> Result<()> {
        // Sequnce of event cids and out of order event heights
        let keys: [u32; 16] = [
            10, 65, 42, 88, 62, 98, 91, 16, 37, 19, 45, 54, 85, 79, 46, 77,
        ];
        let mut tree =
            RedTree::<TestKey, TestValue, recon::BTreeStore<TestKey, TestValue>>::builder()
                .with_b(2)
                .with_bucket_size(4)
                .build(recon::BTreeStore::default());
        for i in keys {
            tree.insert(Entry {
                key: TestKey::new(i),
                value: TestValue(i),
            })
            .await?;
        }
        // This is a simple test to observe that the tree is balanced and that the reduction values
        // are correct.
        expect![[r#"
            RedTree {
                root: Some(
                    Internal(
                        Internal {
                            keys_reduction: "Bucket { reduction: TestValue(65), count: 1 }",
                            keys: [
                                "TestKey(65)",
                            ],
                            children: [
                                Leaf(
                                    Leaf {
                                        keys_reduction: "Bucket { reduction: TestValue(42), count: 1 }",
                                        keys: [
                                            "TestKey(0)",
                                            "TestKey(42)",
                                            "TestKey(65)",
                                        ],
                                        buckets: [
                                            "Bucket { reduction: TestValue(82), count: 4 }",
                                            "Bucket { reduction: TestValue(207), count: 4 }",
                                        ],
                                    },
                                ),
                                Leaf(
                                    Leaf {
                                        keys_reduction: "Bucket { reduction: TestValue(91), count: 1 }",
                                        keys: [
                                            "TestKey(65)",
                                            "TestKey(91)",
                                            "TestKey(4294967295)",
                                        ],
                                        buckets: [
                                            "Bucket { reduction: TestValue(329), count: 4 }",
                                            "Bucket { reduction: TestValue(98), count: 1 }",
                                        ],
                                    },
                                ),
                            ],
                            values: [
                                "Bucket { reduction: TestValue(331), count: 9 }",
                                "Bucket { reduction: TestValue(518), count: 6 }",
                            ],
                        },
                    ),
                ),
                b: 2,
                bucket_size: 4,
            }
        "#]].assert_debug_eq(&tree);
        Ok(())
    }

    #[test(tokio::test)]
    async fn simple_insert_reduce() {
        do_insert_reduce(
            2,
            2,
            (
                vec![1, 2, 3, 4, 5],
                vec![
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
                ],
            ),
        )
        .await;
    }

    async fn do_insert_reduce(
        b: usize,
        bucket_size: usize,
        (mut keys, ranges): (Vec<u16>, Vec<(usize, usize)>),
    ) {
        let mut tree =
            RedTree::<TestKey, TestValue, recon::BTreeStore<TestKey, TestValue>>::builder()
                .with_b(b)
                .with_bucket_size(bucket_size)
                .build(recon::BTreeStore::default());

        // Insert keys in the order ehy exist
        for i in &keys {
            tree.insert(Entry {
                key: TestKey::new(*i as u32),
                value: TestValue(*i as u32),
            })
            .await
            .unwrap();
        }

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
                reduction.0,
            )
        }
    }

    // Produce a set of keys and some ranges over those keys
    fn keys_and_ranges() -> impl Strategy<Value = (Vec<u16>, Vec<(usize, usize)>)> {
        prop::collection::vec(1..u16::MAX - 1, 2..500).prop_flat_map(|vec| {
            let len = vec.len();
            debug!(num = len, "num keys");
            (
                Just(vec),
                prop::collection::vec(0..len - 1, 1..10).prop_flat_map(move |start| {
                    debug!(num = start.len(), "num ranges");
                    start
                        .into_iter()
                        .map(|start| {
                            debug!(start, len, "range");
                            (Just(start), start + 1..len)
                        })
                        .collect::<Vec<(Just<usize>, Range<usize>)>>()
                }),
            )
        })
    }

    proptest! {
        #[test]
        fn insert_reduce(b in 2..100usize, bucket_size in 2..100usize, keys_and_ranges in keys_and_ranges()) {
            tokio::runtime::Runtime::new().unwrap().block_on(do_insert_reduce(b, bucket_size, keys_and_ranges))
        }
    }
}
