use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

/// An open interval from start to end, exclusive on both bounds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RangeOpen<T> {
    /// Exclusive lower bound of the range
    pub start: T,
    /// Exclusive upper bound of the range
    pub end: T,
}

impl<T> RangeBounds<T> for RangeOpen<T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Excluded(&self.start)
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Excluded(&self.end)
    }
}

impl<T> From<(T, T)> for RangeOpen<T> {
    fn from(value: (T, T)) -> Self {
        Self {
            start: value.0,
            end: value.1,
        }
    }
}

impl<T> RangeOpen<T>
where
    T: Ord + Clone,
{
    /// Construct a new [`RangeOpen`] bounds that represents the intersection of two bounds.
    /// When there is no overlapping intersection None is returned.
    pub fn intersect(&self, other: &Self) -> Option<Self> {
        let start = std::cmp::max(&self.start, &other.start);
        let end = std::cmp::min(&self.end, &other.end);
        if start < end {
            Some(Self {
                start: start.clone(),
                end: end.clone(),
            })
        } else {
            None
        }
    }
    /// Map a RangeOpen<T> to a RangeOpen<U>
    pub fn map<U>(self, f: impl Fn(T) -> U) -> RangeOpen<U> {
        RangeOpen {
            start: f(self.start),
            end: f(self.end),
        }
    }
}
