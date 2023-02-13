use arrayvec::ArrayVec;

use crate::Clock;

pub(crate) struct History<C: Clock, const K: usize> {
    times: ArrayVec<C::Time, K>,
    correlated_reference_period: C::Duration,
}

impl<C: Clock, const K: usize> History<C, K> {
    pub fn new(correlated_reference_period: C::Duration) -> Self {
        Self { times: Default::default(), correlated_reference_period }
    }
}

impl<C: Clock, const K: usize> History<C, K> {
    /// The `backwards K'th distance`
    pub fn kth(&self) -> Option<C::Time> {
        // if we don't even have `K` measurements, return None
        if self.times.len() < K { None } else { self.times.first().copied() }
    }

    pub fn mark_access(&mut self, time: C::Time) {
        match self.latest_access() {
            // if it was a correlated reference, then we just update the last reference time
            Some(last) if time - last < self.correlated_reference_period => {
                self.update_latest_access(time)
            }
            // otherwise, we push a new reference time
            _ => self.push(time),
        }
    }

    pub fn latest_access(&self) -> Option<C::Time> {
        self.times.last().copied()
    }

    fn push(&mut self, time: C::Time) {
        if self.times.is_full() {
            self.times.remove(0);
        }
        self.times.push(time);
    }

    fn update_latest_access(&mut self, time: C::Time) {
        self.times.pop().unwrap();
        self.times.push(time);
    }
}
