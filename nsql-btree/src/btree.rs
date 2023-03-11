use std::marker::PhantomData;

use async_recursion::async_recursion;
use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, Serialize};

use crate::node::{Node, NodeKind};
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
    K: Serialize + Deserialize,
    V: Serialize + Deserialize + Clone,
{
    pub async fn create(pool: BufferPool) -> Result<Self> {
        let handle = pool.alloc().await?;
        let root = Node::<K, V>::new_root();
        root.serialize(&mut handle.page().data_mut()).await?;
        let root_idx = handle.page().idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.search_node(self.root_idx, key).await
    }

    // #[async_recursion]
    async fn search_node(&self, idx: PageIndex, key: &K) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let data = handle.page().data();
        let node = PageView::<K, V>::create(&data).await?;
        match node {
            // PageView::Internal(node) => self.search_node(node.search(key), key).await,
            PageView::Leaf(leaf) => leaf.get(key).await,
        }
    }

    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        let handle = self.pool.load(self.root_idx).await?;
        let mut data = handle.page().data_mut();
        let node = PageViewMut::<K, V>::create(&mut data).await?;
        match node {
            PageViewMut::Leaf(mut leaf) => leaf.insert(key, value).await,
        }
    }
}
