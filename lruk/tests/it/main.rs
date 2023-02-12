use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

use lruk::{Clock, LruK};

#[derive(Default)]
struct CounterTimer {
    counter: AtomicU64,
}

impl Clock for CounterTimer {
    type Time = u64;
    type Duration = u64;

    fn now(&self) -> Self::Time {
        self.counter.fetch_add(1, atomic::Ordering::Relaxed)
    }
}

#[test]
fn test_cache() {
    let cache = LruK::<i32, Arc<char>, CounterTimer, 2>::new(2, 0, 0);

    macro_rules! get {
        ($key:expr) => {
            cache.get($key).map(|v| *v.value().as_ref())
        };
    }

    macro_rules! insert {
        ($key:expr => $value:expr) => {
            cache.insert($key, Arc::new($value));
        };
    }

    assert!(cache.is_empty());
    assert!(get!(1).is_none());

    insert!(1 => 'a');

    assert!(!cache.is_empty());
    assert_eq!(cache.len(), 1);
    assert_eq!(get!(1), Some('a'));

    insert!(2 => 'b');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(2), Some('b'));

    insert!(3 => 'c');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), Some('b'));
    assert_eq!(get!(3), Some('c'));

    insert!(4 => 'd');
    assert_eq!(cache.len(), 2);
    assert_eq!(get!(1), None);
    assert_eq!(get!(2), None);
    assert_eq!(get!(3), Some('c'));
    assert_eq!(get!(4), Some('d'));
}
