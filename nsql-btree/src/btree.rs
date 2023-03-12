use std::fmt;
use std::marker::PhantomData;

use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, DeserializeSkip, Serialize};

use crate::page::{PageFull, PageView, PageViewMut};
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
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
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
            PageViewMut::Leaf(mut leaf) => match leaf.insert(key, value).await? {
                Ok(value) => Ok(value),
                Err(PageFull) => {
                    let left_page = self.pool.alloc().await?;
                    let mut left_data = left_page.page().data_mut();
                    PageViewMut::<K, V>::init_leaf(&mut left_data).await?;
                    let left_child =
                        unsafe { PageViewMut::create(&mut left_data).await?.unwrap_leaf() };

                    let right_page = self.pool.alloc().await?;
                    let mut right_data = right_page.page().data_mut();
                    PageViewMut::<K, V>::init_leaf(&mut right_data).await?;
                    let right_child =
                        unsafe { PageViewMut::create(&mut right_data).await?.unwrap_leaf() };

                    leaf.split_root_into(left_child, right_child).await?;
                    todo!();

                    // let (new_key, new_value) = leaf.split(&mut new_leaf).await?;
                    // let new_idx = new_page.page().idx();
                    // let mut parent = PageViewMut::<K, V>::init_root(&mut data).await?;
                    // parent.insert_internal(new_key, new_idx).await?;
                    // Ok(None)
                }
            },
        }
    }
}
