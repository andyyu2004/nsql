use std::borrow::Borrow;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex};

type CacheInner<K, V> = stretto::Cache<
    K,
    Arc<V>,
    stretto::DefaultKeyBuilder<K>,
    ArcCoster<V>,
    stretto::DefaultUpdateValidator<Arc<V>>,
    CacheCallbacks<V>,
>;

pub(crate) struct Cache<K, V> {
    inner: Arc<CacheInner<K, V>>,
    lock: Mutex<()>,
}

struct ArcCoster<V>(PhantomData<V>);

impl<V> Default for ArcCoster<V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

struct CacheCallbacks<V> {
    _phantom: PhantomData<V>,
    evictions_tx: SyncSender<()>,
}

impl<V> CacheCallbacks<V> {
    fn new(evictions_tx: SyncSender<()>) -> Self {
        Self { evictions_tx, _phantom: PhantomData }
    }
}

impl<V: Send + Sync + 'static> stretto::CacheCallback for CacheCallbacks<V> {
    type Value = Arc<V>;

    fn on_exit(&self, _val: Option<Self::Value>) {}

    fn on_evict(&self, item: stretto::Item<Self::Value>) {
        let val = item.val.expect("why is `val` optional?");
        assert_eq!(
            Arc::strong_count(&val),
            1,
            "eviction while there are still references to the `Arc`"
        );
        self.evictions_tx.send(()).expect("unexpected closed eviction receiver");
    }

    fn on_reject(&self, _item: stretto::Item<Self::Value>) {
        panic!("unexpected cache rejection");
    }
}

impl<V: Send + Sync + 'static> stretto::Coster for ArcCoster<V> {
    type Value = Arc<V>;

    fn cost(&self, v: &Arc<V>) -> i64 {
        // if there are still references to the `Arc` then we consider it to have a cost of
        // `0` so that it will never be evicted from the cache
        if Arc::strong_count(v) > 1 { 0 } else { 1 }
    }
}

#[derive(Clone)]
struct KeyBuilder<K>(PhantomData<K>);

impl<K> Default for KeyBuilder<K> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (evictions_tx, evictions_rx) = std::sync::mpsc::sync_channel(capacity);
        let callbacks = CacheCallbacks::new(evictions_tx);
        let inner = Arc::new(
            stretto::Cache::builder(10 * capacity, capacity as i64)
                .set_coster(ArcCoster::default())
                .set_callback(callbacks)
                .finalize()
                .unwrap(),
        );

        let cache = inner.clone();
        std::thread::spawn(move || {
            // each eviction means we can undo the decrement in `max_cost`
            while let Ok(()) = evictions_rx.recv() {
                cache.update_max_cost(cache.max_cost() + 1);
            }
        });

        Self { inner, lock: Default::default() }
    }

    pub fn get<Q>(&self, v: &Q) -> Option<stretto::ValueRef<'_, Arc<V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.inner.get(v)
    }

    pub fn insert(&self, k: K, v: V) {
        // Mutex guard to prevent multiple threads from inserting the same key and messing up the `max_cost` logic
        // It's a bit stupid to add a lock to the otherwise lock free implementation though...
        let _guard = self.lock.lock().unwrap();
        if self.inner.get(&k).is_some() {
            return;
        }
        assert!(self.inner.insert(k, Arc::new(v), 0), "failed to insert into lru cache");
        self.inner.update_max_cost(self.inner.max_cost() - 1);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests;
