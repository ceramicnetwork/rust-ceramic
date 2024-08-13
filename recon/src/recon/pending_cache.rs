use std::collections::HashMap;

use crate::{Key, PendingItem, ReconItem};

#[derive(Debug, Default)]
/// This struct manages tracking items that we attempted to deliver but could not be processed
/// because we need to discover additional information.
pub struct PendingCache<K: Key> {
    by_needed_key: HashMap<Vec<u8>, Vec<ReconItem<K>>>,
    item_keys: std::collections::HashSet<Vec<u8>>,
    max_size: usize,
}

impl<K: Key> PendingCache<K> {
    /// Create a new PendingCache with the given capacity
    pub fn new(max_size: usize) -> Self {
        Self {
            by_needed_key: Default::default(),
            item_keys: Default::default(),
            max_size,
        }
    }

    fn capacity(&self) -> usize {
        self.max_size.saturating_sub(self.size())
    }

    fn size(&self) -> usize {
        debug_assert_eq!(
            self.by_needed_key.values().map(|i| i.len()).sum::<usize>(),
            self.item_keys.len(),
        );
        self.by_needed_key.len()
    }

    /// Stop tracking an item and return it if the key it the item it required shows up
    pub fn remove_by_needed(&mut self, new_item: &ReconItem<K>) -> Option<Vec<ReconItem<K>>> {
        if let Some(ok_now) = self.by_needed_key.remove(new_item.key.as_bytes()) {
            ok_now.iter().for_each(|i| {
                self.item_keys.remove(i.key.as_bytes());
            });
            Some(ok_now)
        } else {
            None
        }
    }

    /// Returns true if we're already tracking the given item
    pub fn is_tracking(&self, item: &ReconItem<K>) -> bool {
        self.item_keys.contains(item.key.as_bytes())
    }

    /// Update our cache to include the new items up to the allowed capacity.
    /// Returns a tuple of (items tracked, capacity remaining)
    pub fn track_pending(&mut self, items: &mut Vec<PendingItem<K>>) -> (usize, usize) {
        let mut tracked = 0;
        for val in items.drain(0..self.capacity().min(items.len())) {
            match self
                .by_needed_key
                .entry(val.required_key.as_bytes().to_vec())
            {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    self.item_keys.insert(val.item.key.as_bytes().to_vec());
                    o.get_mut().push(val.item);
                    tracked += 1;
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    self.item_keys.insert(val.item.key.as_bytes().to_vec());
                    v.insert(vec![val.item]);
                    tracked += 1;
                }
            }
        }

        (tracked, self.capacity())
    }
}

#[cfg(test)]
mod test {

    use crate::tests::AlphaNumBytes;
    use test_log::test;

    use super::*;

    const REQUIRED_OFFSET: usize = 1_000_000;
    fn get_items(num: usize) -> Vec<PendingItem<AlphaNumBytes>> {
        let mut res = Vec::with_capacity(num);
        for i in 0..num {
            let id = AlphaNumBytes::from(format!("{}", i));
            let req = required_item(i);
            let item = ReconItem::new(id, i.to_le_bytes().to_vec());
            res.push(PendingItem::new(req.key, item));
        }

        res
    }

    fn required_item(i: usize) -> ReconItem<AlphaNumBytes> {
        let req = AlphaNumBytes::from(format!("{}", i + REQUIRED_OFFSET));
        let val = req.as_bytes().to_vec();
        ReconItem::new(req, val)
    }

    fn cache_and_assert(
        cache: &mut PendingCache<AlphaNumBytes>,
        mut items: Vec<PendingItem<AlphaNumBytes>>,
        expected_cached: usize,
    ) {
        let expected_items = items.clone();
        let expected_cache_size = cache.size() + expected_cached;
        let expected_remaining = items.len() - expected_cached;

        cache.track_pending(&mut items);

        assert_eq!(expected_cache_size, cache.item_keys.len());
        assert_eq!(expected_cache_size, cache.by_needed_key.len());
        assert_eq!(expected_remaining, items.len(), "{:?}", items);

        for (i, v) in expected_items.into_iter().take(expected_cached).enumerate() {
            assert!(
                cache.is_tracking(&v.item),
                "not tracking: {:?} {:?}",
                v,
                cache
            );

            let req = required_item(i);
            let cached = cache.remove_by_needed(&req).unwrap_or_else(|| {
                panic!("should have cached {:?} by {:?} cache={:?}", v, req, cache)
            });

            assert_eq!(vec![v.item], cached);
        }

        assert_eq!(0, cache.item_keys.len());
        assert_eq!(0, cache.by_needed_key.len());
    }

    #[test]
    fn pending_caches_max() {
        let mut cache = PendingCache::new(10);
        let items = get_items(10);
        cache_and_assert(&mut cache, items, 10);
    }

    #[test]
    fn pending_caches_with_space() {
        let mut cache = PendingCache::new(20);
        let items = get_items(10);
        cache_and_assert(&mut cache, items, 10);
        let items = get_items(5);
        cache_and_assert(&mut cache, items, 5);
    }

    #[test]
    fn pending_caches_drops_too_many() {
        let mut cache = PendingCache::new(10);
        let items = get_items(20);
        cache_and_assert(&mut cache, items, 10);
    }

    #[test]
    fn pending_caches_drops_too_many_need_same_key() {
        let expected_cached = 10;
        let mut cache = PendingCache::new(expected_cached);
        let mut items = Vec::with_capacity(20);

        fn required_item_dup_keys(i: usize) -> ReconItem<AlphaNumBytes> {
            let req = if i % 2 == 0 {
                // all even depends on the same item
                AlphaNumBytes::from(format!("{}", 1_000))
            } else {
                AlphaNumBytes::from(format!("{}", i + REQUIRED_OFFSET))
            };
            let req_item = req.as_bytes().to_vec();
            ReconItem::new(req, req_item)
        }

        for i in 0..20 {
            let id = AlphaNumBytes::from(format!("{}", i));
            let req = required_item_dup_keys(i);
            let item = ReconItem::new(id, i.to_le_bytes().to_vec());
            items.push(PendingItem::new(req.key, item));
        }
        let expected_cached = 10;

        let expected_items: Vec<_> = items.clone().into_iter().take(expected_cached).collect();
        let expected_cache_size = cache.size() + expected_cached;
        let expected_remaining = items.len() - expected_cached;

        cache.track_pending(&mut items);

        assert_eq!(expected_cache_size, cache.item_keys.len());
        assert_eq!(
            expected_cache_size,
            cache.by_needed_key.values().map(|i| i.len()).sum::<usize>()
        );
        assert_eq!(expected_remaining, items.len(), "{:?}", items);

        // cached everything we expected
        for v in expected_items.iter() {
            assert!(
                cache.is_tracking(&v.item),
                "not tracking: {:?} {:?}",
                v,
                cache
            );
        }

        // odd items are all a vec of one item
        for (i, v) in expected_items.iter().enumerate() {
            if i % 2 == 0 {
                continue;
            }
            let req = required_item_dup_keys(i);
            let cached = cache.remove_by_needed(&req).unwrap_or_else(|| {
                panic!("should have cached {:?} by {:?} cache={:?}", v, req, cache)
            });

            assert_eq!(vec![v.item.clone()], cached);
        }

        // the even items are under one key
        let req = required_item_dup_keys(0); // anything even
        let cached = cache
            .remove_by_needed(&req)
            .unwrap_or_else(|| panic!("should have cached vec by {:?} cache={:?}", req, cache));

        let even: Vec<_> =
            expected_items
                .into_iter()
                .enumerate()
                .fold(vec![], |mut even, (i, v)| {
                    if i % 2 == 0 {
                        even.push(v.item);
                    }
                    even
                });
        assert_eq!(even, cached);

        assert_eq!(0, cache.item_keys.len());
        assert_eq!(0, cache.by_needed_key.len());
    }
}
