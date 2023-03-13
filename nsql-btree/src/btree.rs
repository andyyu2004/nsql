use std::fmt;
use std::marker::PhantomData;

use async_recursion::async_recursion;
use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, DeserializeSkip, Serialize};

use crate::node::Flags;
use crate::page::{PageFull, PageView, PageViewMut, PageViewMutKind};
use crate::Result;

/// A B+ tree
pub struct BTree<K, V> {
    pool: BufferPool,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

// hack trait to patch some implementation holes for now
pub trait Max {
    const MAX: Self;
}

impl Max for u32 {
    const MAX: Self = u32::MAX;
}

impl<K, V> BTree<K, V>
where
    K: Ord + Send + Sync + Max,
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
    V: Serialize + Deserialize + Eq + Clone + fmt::Debug,
{
    #[inline]
    pub async fn create(pool: BufferPool) -> Result<Self> {
        let handle = pool.alloc().await?;
        let page = handle.page();
        let mut data = page.data_mut();

        PageViewMut::<K, V>::init_root_leaf(&mut data).await?;
        let root_idx = page.idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.search_node(self.root_idx, key).await
    }

    #[async_recursion(?Send)]
    #[inline]
    async fn search_node(&self, idx: PageIndex, key: &K) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let data = handle.page().data();
        let node = unsafe { PageView::<K, V>::create(&data).await? };
        match node {
            PageView::Leaf(leaf) => leaf.get(key).await,
            PageView::Interior(interior) => {
                let child_idx = interior.search(key).await?;
                self.search_node(child_idx, key).await
            }
        }
    }

    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        self.insert_rec(self.root_idx, key, value).await
    }

    #[inline]
    async fn insert_rec(&self, page_idx: PageIndex, key: K, value: V) -> Result<Option<V>> {
        let handle = self.pool.load(page_idx).await?;
        let mut data = handle.page().data_mut();
        let view = unsafe { PageViewMut::<K, V>::create(&mut data).await? };
        match view.kind {
            PageViewMutKind::Interior(_) => todo!(),
            PageViewMutKind::Leaf(mut leaf) => match leaf.insert(&key, &value).await? {
                Ok(value) => Ok(value),
                Err(PageFull) => {
                    if view.header.flags.contains(Flags::IS_ROOT) {
                        let left_page = self.pool.alloc().await?;
                        let mut left_data = left_page.page().data_mut();
                        let mut left_child = PageViewMut::<K, V>::init_leaf(&mut left_data).await?;

                        let right_page = self.pool.alloc().await?;
                        let mut right_data = right_page.page().data_mut();
                        let mut right_child =
                            PageViewMut::<K, V>::init_leaf(&mut right_data).await?;

                        let sep = leaf.split_root_into(&mut left_child, &mut right_child).await?;

                        (if key < sep { left_child } else { right_child })
                            .insert(&key, &value)
                            .await?
                            .expect("split child should not be full");

                        // reinitialize the root to an interior node and add the two children
                        let mut root = PageViewMut::<K, V>::init_root_interior(&mut data).await?;

                        let hack_high_key = K::MAX;

                        root.insert(
                            &sep,
                            left_page.page().idx(),
                            right_page.page().idx(),
                            &hack_high_key,
                        )
                        .await?
                        .expect("new root should not be full");

                        Ok(None)
                    } else {
                        todo!()
                    }
                }
            },
        }
    }
}
