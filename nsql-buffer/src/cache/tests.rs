use super::Cache;

#[test]
fn test_cache() {
    let cache = Cache::<i32, char>::new(20);
    macro_rules! get {
        ($key:expr) => {
            cache.get(&$key).map(|v| *v.value().as_ref())
        };
    }

    assert!(cache.is_empty());
    assert!(get!(1).is_none());

    cache.insert(1, 'a');
    assert!(!cache.is_empty());
    assert_eq!(!cache.len(), 1);
    assert_eq!(get!(1), Some('a'));
}
