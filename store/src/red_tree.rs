use anyhow::Result;
use async_recursion::async_recursion;
use ceramic_core::{EventId, RangeOpen};
use recon::{AssociativeHash, HashCount, Key, ReconItem, Sha256a};
use tracing::debug;

/// Red(uction)Tree is a variant of BTree that is only the top portion of a btree.
/// The tree is in memory and can serve queries about the large ranges of the tree but delegates to
/// another implementation for more fine grained ranges of the tree.
///
/// It is named a Reduction Tree because the only query supported is a reduction of values within a
/// range. The value must be associative and communitative so that it can be partially reduced and
/// store on intermediate nodes.
pub struct RedTree<S> {
    root: Option<Node>,
    b: usize,
    bucket_size: u64,
    store: S,
}

impl<S> std::fmt::Debug for RedTree<S> {
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
    bucket_size: u64,
}

impl Builder {
    pub fn with_b(self, b: usize) -> Self {
        Self { b, ..self }
    }
    pub fn with_bucket_size(self, bucket_size: u64) -> Self {
        Self {
            bucket_size,
            ..self
        }
    }
    pub fn build<S>(self, store: S) -> RedTree<S>
    where
        S: recon::Store<Key = EventId, Hash = Sha256a>,
    {
        RedTree {
            root: None,
            store,
            b: self.b,
            bucket_size: self.bucket_size,
        }
    }
}

#[derive(Clone)]
pub struct Entry {
    key: EventId,
    value: HashCount<Sha256a>,
}

impl Entry {
    /// Construct a new entry
    pub fn new(key: EventId, value: HashCount<Sha256a>) -> Self {
        Self { key, value }
    }
}

impl<S> RedTree<S> {
    /// Create a builder for constructing a RedTree.
    pub fn builder() -> Builder {
        Builder {
            b: 16,
            bucket_size: 2_u64.pow(15),
        }
    }
}
impl<S> RedTree<S>
where
    S: recon::Store<Key = EventId, Hash = Sha256a> + Send,
{
    /// Insert a ReconItem into the store
    pub async fn insert(&mut self, item: ReconItem<'_, EventId>) -> Result<bool> {
        // First update the RedTree
        self.insert_hash(Entry {
            key: item.key.clone(),
            value: Sha256a::digest(item.key).into(),
        })
        .await?;
        // Second insert into the store.
        self.store.insert(item).await
    }
    /// Insert a key value pair possibly splitting nodes along the way.
    async fn insert_hash(&mut self, kv: Entry) -> Result<()> {
        if let Some(mut root) = self.root.take() {
            if self.is_node_full(&root) {
                // split the root creating a new root and child nodes along the way.
                let (median, sibling) = root.split(self.b);
                let root_value = root.reduce();
                let sibling_value = sibling.reduce();
                let mut new_root = Internal::new(self.b);
                new_root.children.push(root);
                new_root.children.push(sibling);
                new_root.keys.push(median);
                new_root.values.push(root_value);
                new_root.values.push(sibling_value);
                root = Node::Internal(new_root);
            }
            let ret = self.insert_non_full(&mut root, kv).await;
            self.root = Some(root);
            ret
        } else {
            let mut root = Node::Leaf(Leaf::new(self.b));
            let ret = self.insert_non_full(&mut root, kv).await;
            self.root = Some(root);
            ret
        }
    }

    /// insert_non_full (recursively) finds a node rooted at a given non-full node.
    /// to insert a given key-value pair.
    #[async_recursion]
    async fn insert_non_full(&mut self, node: &mut Node, kv: Entry) -> Result<()> {
        match node {
            Node::Internal(node) => {
                let idx = node.keys.binary_search(&kv.key).unwrap_or_else(|x| x);
                let child = &mut node.children[idx];
                if self.is_node_full(&child) {
                    let (median, mut sibling) = child.split(self.b);

                    // Recurse
                    let ret = if kv.key <= median {
                        self.insert_non_full(child, kv).await
                    } else {
                        self.insert_non_full(&mut sibling, kv).await
                    };

                    // Now that both child and sibling are udpated we can update the current node.

                    // Update/Insert values for new children
                    node.values[idx] = child.reduce();
                    node.values.insert(idx + 1, sibling.reduce());

                    // Siblings keys are larger than the original child thus need to be inserted
                    // at the next index.
                    node.children.insert(idx + 1, sibling);
                    node.keys.insert(idx, median.clone());

                    ret
                } else {
                    // Child is not full, simply recurse and update its value
                    let ret = self.insert_non_full(child, kv.clone()).await;
                    if ret.is_ok() {
                        // Only update our local value if we are successful in inserting the value.
                        node.values[idx] = node.values[idx].clone() + kv.value;
                    }
                    ret
                }
            }
            Node::Leaf(node) => {
                let idx = node.keys.binary_search(&kv.key).unwrap_or_else(|x| x);
                let bucket = &node.buckets[idx];
                if bucket.count() >= self.bucket_size {
                    // Bucket is full, we need to create a new bucket.
                    // Need to split a bucket which means querying storage for the middle of the
                    // range and the hashes.
                    let start = &node
                        .keys
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| EventId::min_value());
                    let end = &node
                        .keys
                        .get(idx + 1)
                        .cloned()
                        .unwrap_or_else(|| EventId::max_value());
                    let middle = self
                        .store
                        .middle(start, end)
                        .await?
                        .expect("should have a middle key as the range should not be empty");
                    let left_hash = self.store.hash_range(start, &middle).await?;
                    let right_hash = self.store.hash_range(&middle, end).await?;
                    debug!( ?node.buckets, ?idx, ?left_hash, ?right_hash, ?middle, "split bucket");
                    node.buckets[idx] = left_hash;
                    node.buckets.insert(idx + 1, right_hash);
                    node.keys.insert(idx, middle);
                } else {
                    node.buckets[idx] = bucket.clone() + kv.value.clone();
                }
                Ok(())
            }
        }
    }

    fn is_node_full(&self, node: &Node) -> bool {
        match node {
            Node::Internal(Internal { keys, .. }) => keys.len() == (2 * self.b - 1),
            Node::Leaf(Leaf {
                buckets: values, ..
            }) => values.len() == (2 * self.b - 1),
        }
    }

    pub async fn reduce(&self, range: RangeOpen<EventId>) -> HashCount<Sha256a> {
        if let Some(root) = &self.root {
            self.reduce_node(&root, &range).1
        } else {
            HashCount::default()
        }
    }

    fn reduce_node(
        &self,
        node: &Node,
        search: &RangeOpen<EventId>,
    ) -> (RangeControl, HashCount<Sha256a>) {
        match node {
            Node::Internal(node) => {
                let start = node.keys.binary_search(&search.start).unwrap_or_else(|x| x);
                let child = &node.children[start];
                let (ctl, hash) = self.reduce_node(&child, search);
                match ctl {
                    RangeControl::Exclude => (RangeControl::Exclude, hash),
                    RangeControl::Include => {
                        let end = node.keys.binary_search(&search.end).unwrap_or_else(|x| x);
                        let included_children = &node.children[start + 1..=end];
                        let mut hash = HashCount::default();
                        for child in included_children {
                            hash = hash + self.reduce_node_include(&child, &search.end);
                        }
                        if included_children.is_empty() {
                            (RangeControl::Exclude, hash)
                        } else {
                            (RangeControl::Include, hash)
                        }
                    }
                }
            }
            Node::Leaf(_) => todo!(),
        }
    }
    // Reduce all values in this node and child nodes up to end
    fn reduce_node_include(&self, node: &Node, end: &EventId) -> HashCount<Sha256a> {
        match node {
            Node::Internal(node) => {
                let mut hash = HashCount::default();
                let idx = node.keys.binary_search(end).unwrap_or_else(|x| x);
                // Do not recurse into children that are fully covered by the range
                for child_value in &node.values[0..idx] {
                    debug!("skipping recursion into child");
                    // TODO add AddAssign to the AssociativeHash trait or similar
                    hash = hash + child_value.clone();
                }
                // Recurse into last matching child as it may be only a partial match
                let child = &node.children[idx];
                hash + self.reduce_node_include(child, end)
            }
            Node::Leaf(_) => todo!(),
        }
    }
}

enum RangeControl {
    Exclude,
    Include,
}

// Node within the tree.
#[derive(Debug)]
enum Node {
    Internal(Internal),
    Leaf(Leaf),
}

impl Node {
    /// Split creates a sibling node from a given node by splitting the node in two around a median.
    /// split will split the child at b leaving the [0, b-1] keys
    /// while moving the set of [b, 2b-1] keys to the sibling.
    fn split(&mut self, b: usize) -> (EventId, Node) {
        match self {
            Node::Internal(Internal {
                keys,
                children,
                values,
            }) => {
                // Populate siblings keys.
                let mut sibling_keys = keys.split_off(b - 1);
                // Pop median key - to be added to the parent..
                let median_key = sibling_keys.remove(0);
                // Populate siblings children.
                let sibling_children = children.split_off(b);
                // Populate siblings values.
                let sibling_values = values.split_off(b);
                (
                    median_key,
                    Node::Internal(Internal {
                        keys: sibling_keys,
                        children: sibling_children,
                        values: sibling_values,
                    }),
                )
            }

            Node::Leaf(Leaf {
                keys,
                buckets: values,
            }) => {
                // Populate siblings keys.
                let mut sibling_keys = keys.split_off(b - 1);
                // Pop median key - to be added to the parent..
                let median_key = sibling_keys.remove(0);
                // Populate siblings values.
                let sibling_values = values.split_off(b);
                (
                    median_key,
                    Node::Leaf(Leaf {
                        keys: sibling_keys,
                        buckets: sibling_values,
                    }),
                )
            }
        }
    }

    fn reduce(&self) -> HashCount<Sha256a> {
        match self {
            Node::Internal(Internal { values, .. })
            | Node::Leaf(Leaf {
                buckets: values, ..
            }) => values
                .iter()
                .fold(HashCount::default(), |acc, hash| acc + hash.clone()),
        }
    }
}

struct Internal {
    // Keys that split children, keys.len() == children.len() - 1 always
    keys: Vec<EventId>,
    // List of children of the node
    children: Vec<Node>,
    // Reduced values for each child
    values: Vec<HashCount<Sha256a>>,
}

impl Internal {
    fn new(b: usize) -> Internal {
        let size = 2 * b - 1;
        Internal {
            keys: Vec::with_capacity(size - 1),
            children: Vec::with_capacity(size),
            values: Vec::with_capacity(size),
        }
    }
}
impl std::fmt::Debug for Internal {
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
struct Leaf {
    // Keys that split buckets, keys.len() == buckets.len() - 1 always
    keys: Vec<EventId>,
    // Reduced values for each child
    buckets: Vec<HashCount<Sha256a>>,
}

impl Leaf {
    fn new(b: usize) -> Leaf {
        let size = 2 * b - 1;
        let mut buckets = Vec::with_capacity(size);
        buckets.push(HashCount::default());
        Leaf {
            keys: Vec::with_capacity(size),
            buckets,
        }
    }
}

impl std::fmt::Debug for Leaf {
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

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use test_log::test;

    use crate::tests::random_event_id;

    use super::*;

    #[test(tokio::test)]
    async fn insert() -> Result<()> {
        let mut tree = RedTree::<recon::BTreeStore<EventId, Sha256a>>::builder()
            .with_b(2)
            .with_bucket_size(4)
            .build(recon::BTreeStore::default());
        for _ in 0..16 {
            tree.insert(ReconItem::new_key(&random_event_id(None, None)))
                .await?;
        }
        expect![""].assert_debug_eq(&tree);
        Ok(())
    }
}
