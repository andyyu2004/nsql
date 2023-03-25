use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_recursion::async_recursion;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_rkyv::DefaultSerializer;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::{
    Flags, InteriorPageViewMut, LeafPageViewMut, NodeMut, NodeView, NodeViewMut, PageFull,
    PageView, PageViewMut,
};
use crate::Result;

/// A B+ tree
pub struct BTree<K, V> {
    pool: Arc<dyn Pool>,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> fmt::Debug for BTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BTree").field("root_idx", &self.root_idx).finish()
    }
}

impl<K, V> Clone for BTree<K, V> {
    fn clone(&self) -> Self {
        Self { pool: Arc::clone(&self.pool), root_idx: self.root_idx, marker: PhantomData }
    }
}

impl<K, V> BTree<K, V>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug + 'static,
    K::Archived: Deserialize<K, rkyv::Infallible> + PartialOrd<K> + fmt::Debug + Ord,
    V: Archive + Serialize<DefaultSerializer> + fmt::Debug + 'static,
    V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    #[inline]
    pub async fn initialize(pool: Arc<dyn Pool>) -> Result<Self> {
        let handle = pool.alloc().await?;
        let mut guard = handle.write().await;

        let _root = LeafPageViewMut::<K, V>::initialize_root(&mut guard);

        let root_idx = handle.page_idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    #[tracing::instrument]
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = nsql_rkyv::to_bytes(key);
        let archived_key = unsafe { rkyv::archived_root::<K>(&key_bytes) };
        self.search_node(self.root_idx, archived_key).await
    }

    /// Insert a key-value pair into the tree returning the previous value if it existed
    #[inline]
    #[tracing::instrument]
    pub async fn insert(&self, key: &K, value: &V) -> Result<Option<V>> {
        let (leaf_idx, parents) = self.find_leaf_page_idx(key).await?;
        self.insert_leaf(leaf_idx, parents, key, value).await
    }

    #[async_recursion(?Send)]
    async fn search_node(&self, idx: PageIndex, key: &K::Archived) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let guard = handle.read().await;
        let node = unsafe { PageView::<K, V>::view(&guard).await? };
        match node {
            PageView::Leaf(leaf) => Ok(leaf.get(key).map(nsql_rkyv::deserialize)),
            PageView::Interior(interior) => {
                let child_idx = interior.search(key);
                drop(guard);
                self.search_node(child_idx, key).await
            }
        }
    }

    /// Find the leaf page that should contain the given key, and return the index of that page and
    /// stack of parent page indices.
    async fn find_leaf_page_idx(&self, key: &K) -> Result<(PageIndex, Vec<PageIndex>)> {
        let mut parents = vec![];
        let leaf_idx = self.find_leaf_page_idx_rec(self.root_idx, key, &mut parents).await?;
        Ok((leaf_idx, parents))
    }

    #[async_recursion(?Send)]
    async fn find_leaf_page_idx_rec(
        &self,
        idx: PageIndex,
        key: &K,
        stack: &mut Vec<PageIndex>,
    ) -> Result<PageIndex> {
        let handle = self.pool.load(idx).await?;
        let guard = handle.read().await;
        let node = unsafe { PageView::<K, V>::view(&guard).await? };
        match node {
            PageView::Leaf(_) => Ok(idx),
            PageView::Interior(interior) => {
                stack.push(idx);
                let child_idx = interior.search(key);
                drop(guard);
                self.find_leaf_page_idx_rec(child_idx, key, stack).await
            }
        }
    }

    #[inline]
    async fn insert_leaf(
        &self,
        leaf_page_idx: PageIndex,
        parents: Vec<PageIndex>,
        key: &K,
        value: &V,
    ) -> Result<Option<V>> {
        let leaf_handle = self.pool.load(leaf_page_idx).await?;
        let mut leaf_guard = leaf_handle.write().await;
        let view = unsafe { PageViewMut::<K, V>::view_mut(&mut leaf_guard).await? };
        let mut leaf = match view {
            PageViewMut::Interior(_) => unreachable!("should have been passed a leaf page"),
            PageViewMut::Leaf(leaf) => leaf,
        };

        match leaf.insert(key, value) {
            Ok(value) => Ok(value),
            Err(PageFull) => {
                if leaf.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_leaf_split);
                    self.split_root_and_insert::<LeafPageViewMut<'_, K, V>, _>(leaf, key, value)
                        .await
                } else {
                    cov_mark::hit!(non_root_leaf_split);
                    self.split_non_root_and_insert::<LeafPageViewMut<'_, K, V>, _>(
                        parents,
                        leaf_page_idx,
                        leaf,
                        key,
                        value,
                    )
                    .await
                }
            }
        }
    }

    #[async_recursion(?Send)]
    async fn insert_interior(
        &self,
        mut parents: Vec<PageIndex>,
        key: &K,
        child_idx: PageIndex,
    ) -> Result<()> {
        let parent_idx = parents.pop().expect("non-root leaf must have at least one parent");
        let parent_page = self.pool.load(parent_idx).await?;
        let mut parent_guard = parent_page.write().await;
        let parent_view = unsafe { PageViewMut::<K, V>::view_mut(&mut parent_guard).await? };
        let mut parent = match parent_view {
            PageViewMut::Interior(interior) => interior,
            PageViewMut::Leaf(_) => {
                unreachable!("parent should be interior page")
            }
        };

        match parent.insert(key, &child_idx) {
            Ok(None) => {}
            Ok(Some(_value)) => todo!("duplicate key in interior node"),
            Err(PageFull) => {
                if parent.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_interior_split);
                    self.split_root_and_insert::<InteriorPageViewMut<'_, K>, _>(
                        parent, key, &child_idx,
                    )
                    .await?;
                } else {
                    cov_mark::hit!(non_root_interior_split);
                    self.split_non_root_and_insert::<InteriorPageViewMut<'_, K>, _>(
                        parents, parent_idx, parent, key, &child_idx,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn split_non_root_and_insert<N, T>(
        &self,
        parents: Vec<PageIndex>,
        node_page_idx: PageIndex,
        mut node: N::ViewMut<'_>,
        key: &K,
        value: &T,
    ) -> Result<Option<T>>
    where
        N: NodeMut<K, T>,
        T: Archive + Serialize<DefaultSerializer> + fmt::Debug,
        T::Archived: Deserialize<T, rkyv::Infallible> + fmt::Debug,
    {
        // split the non-root interior by allocating a new interior and splitting the contents
        // then we insert the separator key and the new page index into the parent
        let new_page = self.pool.alloc().await?;
        let mut new_guard = new_page.page().write().await;
        let new_page_idx = new_guard.page_idx();
        let mut new_node = N::initialize(&mut new_guard);

        N::split_left_into(&mut node, new_page_idx, &mut new_node, node_page_idx);

        let sep = new_node.low_key().unwrap();
        // FIXME ideally we don't have to deserialize sep
        self.insert_interior(parents, &nsql_rkyv::deserialize(sep), new_page_idx).await?;

        // insert the new separator key and child index into the parent
        let prev = (if sep > key { node.insert(key, value) } else { new_node.insert(key, value) })
            .expect("split interior should not be full");

        Ok(prev)
    }

    async fn split_root_and_insert<N, T>(
        &self,
        mut root: N::ViewMut<'_>,
        key: &K,
        value: &T,
    ) -> Result<Option<T>>
    where
        N: NodeMut<K, T>,
        T: Archive + Serialize<DefaultSerializer> + fmt::Debug,
        T::Archived: Deserialize<T, rkyv::Infallible> + fmt::Debug,
    {
        assert!(root.len() >= 3, "root that requires a split should contain at least 3 entries");
        let left_page = self.pool.alloc().await?;
        let mut left_guard = left_page.write().await;

        let right_guard = self.pool.alloc().await?;
        let mut right_guard = right_guard.write().await;

        let left_page_idx = left_guard.page_idx();
        let right_page_idx = right_guard.page_idx();
        let mut left_child = N::initialize(&mut left_guard);
        let mut right_child = N::initialize(&mut right_guard);

        N::split_root_into(
            &mut root,
            left_page_idx,
            &mut left_child,
            right_page_idx,
            &mut right_child,
        );

        let sep = right_child.low_key().unwrap();

        // reinitialize the root to an interior root node and add the two children
        let mut root = root.reinitialize_as_root_interior();
        root.insert(&K::MIN, &left_page_idx).expect("new root should not be full");
        // FIXME ideally we don't have to deserialize sep
        root.insert(&nsql_rkyv::deserialize(sep), &right_page_idx)
            .expect("new root should not be full");

        let prev = (if sep > key { left_child } else { right_child })
            .insert(key, value)
            .expect("split child should not be full");
        Ok(prev)
    }
}

// hack to generate "negative infinity" low keys for L&Y trees for left-most nodes
pub trait Min: Archive {
    const MIN: Self;
}

impl Min for u8 {
    const MIN: Self = 0;
}

macro_rules! impl_min_numeric {
    ($($ty:ty),*) => {
        $(
            impl Min for $ty {
                const MIN: Self = 0;
            }
        )*
    };
}

impl_min_numeric!(i16, i32, i64, u16, u32, u64);
