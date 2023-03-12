use std::fmt;
use std::marker::PhantomData;

use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, Serialize};

use crate::page::{PageView, PageViewMut};
use crate::Result;

/// A B+ tree
pub struct BTree<K, V> {
    pool: BufferPool,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Ord + Send + Sync,
    K: Serialize + Deserialize + Ord + fmt::Debug,
    V: Serialize + Deserialize + Eq + Clone + fmt::Debug,
{
    #[inline]
    pub async fn create(pool: BufferPool) -> Result<Self> {
        let handle = pool.alloc().await?;
        let page = handle.page();
        let mut data = page.data_mut();

        PageViewMut::<K, V>::init_root(&mut data).await?;
        let root_idx = page.idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.search_node(self.root_idx, key).await
    }

    // #[async_recursion]
    #[inline]
    async fn search_node(&self, idx: PageIndex, key: &K) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let data = handle.page().data();
        let node = unsafe { PageView::<K, V>::create(&data).await? };
        match node {
            // PageView::Internal(node) => self.search_node(node.search(key), key).await,
            PageView::Leaf(leaf) => leaf.get(key).await,
        }
    }

    #[inline]
    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        let handle = self.pool.load(self.root_idx).await?;
        let mut data = handle.page().data_mut();
        let node = unsafe { PageViewMut::<K, V>::create(&mut data).await? };
        match node {
            PageViewMut::Leaf(mut leaf) => leaf.insert(key, value).await,
        }
    }
}
