use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_recursion::async_recursion;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_rkyv::DefaultSerializer;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::{
    Flags, InteriorNode, LeafNode, NodeMut, NodeView, NodeViewMut, PageFull, PageView, PageViewMut,
};
use crate::Result;

/// A B+ tree
pub struct BTree<K, V> {
    pool: Arc<dyn Pool>,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for BTree<K, V> {
    fn clone(&self) -> Self {
        Self { pool: Arc::clone(&self.pool), root_idx: self.root_idx, marker: PhantomData }
    }
}

impl<K, V> BTree<K, V>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug,
    K::Archived: PartialOrd<K> + Clone + fmt::Debug + Ord,
    V: Archive + Serialize<DefaultSerializer> + Clone + fmt::Debug,
    V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    #[inline]
    pub async fn initialize(pool: Arc<dyn Pool>) -> Result<Self> {
        let handle = pool.alloc().await?;
        let mut data = handle.write().await;

        let _root = LeafNode::<K, V>::initialize_root(&mut data);

        let root_idx = handle.page_idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = nsql_rkyv::to_bytes(key);
        let archived_key = unsafe { rkyv::archived_root::<K>(&key_bytes) };
        self.search_node(self.root_idx, archived_key).await
    }

    #[inline]
    pub async fn insert(&self, key: &K, value: &V) -> Result<()> {
        let key_bytes = nsql_rkyv::to_bytes(key);
        let archived_key = unsafe { rkyv::archived_root::<K>(&key_bytes) };
        let value_bytes = nsql_rkyv::to_bytes(value);
        let archived_value = unsafe { rkyv::archived_root::<V>(&value_bytes) };
        self.insert_archived(archived_key, archived_value).await
    }

    #[async_recursion(?Send)]
    async fn search_node(&self, idx: PageIndex, key: &K::Archived) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let data = handle.read().await;
        let node = unsafe { PageView::<K, V>::view(&data).await? };
        match node {
            PageView::Leaf(leaf) => Ok(leaf.get(key).map(nsql_rkyv::deserialize)),
            PageView::Interior(interior) => {
                let child_idx = interior.search(key);
                self.search_node(child_idx, key).await
            }
        }
    }

    async fn find_page_idx(&self, key: &K::Archived) -> Result<(PageIndex, Vec<PageIndex>)> {
        let mut parents = vec![];
        let leaf_idx = self.find_page_idx_rec(self.root_idx, key, &mut parents).await?;
        Ok((leaf_idx, parents))
    }

    #[async_recursion(?Send)]
    async fn find_page_idx_rec(
        &self,
        idx: PageIndex,
        key: &K::Archived,
        stack: &mut Vec<PageIndex>,
    ) -> Result<PageIndex> {
        let handle = self.pool.load(idx).await?;
        let data = handle.read().await;
        let node = unsafe { PageView::<K, V>::view(&data).await? };
        match node {
            PageView::Leaf(_) => Ok(idx),
            PageView::Interior(interior) => {
                stack.push(idx);
                let child_idx = interior.search(key);
                self.find_page_idx_rec(child_idx, key, stack).await
            }
        }
    }

    async fn insert_archived(&self, key: &K::Archived, value: &V::Archived) -> Result<()> {
        let (leaf_idx, parents) = self.find_page_idx(key).await?;
        self.insert_inner(leaf_idx, parents, key, value).await
    }

    #[inline]
    async fn insert_inner(
        &self,
        leaf_page_idx: PageIndex,
        parents: Vec<PageIndex>,
        key: &K::Archived,
        value: &V::Archived,
    ) -> Result<()> {
        let leaf_handle = self.pool.load(leaf_page_idx).await?;
        let mut leaf_data = leaf_handle.write().await;
        let view = unsafe { PageViewMut::<K, V>::view_mut(&mut leaf_data).await? };
        let mut leaf = match view {
            PageViewMut::Interior(_) => unreachable!("should have been passed a leaf page"),
            PageViewMut::Leaf(leaf) => leaf,
        };

        match leaf.insert(key.clone(), value.clone()) {
            Ok(value) => Ok(value),
            Err(PageFull) => {
                if leaf.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_leaf_split);
                    assert!(leaf.len() >= 3, "pages should always contain at least 3 entries");

                    self.split_root::<LeafNode<K, V>, _>(leaf, key.clone(), value.clone()).await
                } else {
                    cov_mark::hit!(non_root_leaf_split);

                    self.split_non_root::<LeafNode<K, V>, _>(
                        parents,
                        leaf_page_idx,
                        leaf,
                        key.clone(),
                        value.clone(),
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
        key: &K::Archived,
        child_idx: PageIndex,
    ) -> Result<()> {
        let parent_idx = parents.pop().expect("non-root leaf must have at least one parent");
        let parent_page = self.pool.load(parent_idx).await?;
        let mut parent_data = parent_page.write().await;
        let parent_view = unsafe { PageViewMut::<K, V>::view_mut(&mut parent_data).await? };
        let mut parent = match parent_view {
            PageViewMut::Interior(interior) => interior,
            PageViewMut::Leaf(_) => {
                unreachable!("parent should be interior page")
            }
        };

        match parent.insert(key.clone(), child_idx) {
            Ok(()) => {}
            Err(PageFull) => {
                if parent.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_interior_split);
                    self.split_root::<InteriorNode<K>, _>(parent, key.clone(), child_idx.into())
                        .await?;
                } else {
                    cov_mark::hit!(non_root_interior_split);
                    self.split_non_root::<InteriorNode<K>, _>(
                        parents,
                        parent_idx,
                        parent,
                        key.clone(),
                        child_idx.into(),
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn split_non_root<N, T>(
        &self,
        parents: Vec<PageIndex>,
        node_page_idx: PageIndex,
        mut node: N::ViewMut<'_>,
        // mut new_data: nsql_pager::PageWriteGuard<'_>,
        key: K::Archived,
        value: T::Archived,
    ) -> Result<()>
    where
        N: NodeMut<K, T>,
        T: Archive + fmt::Debug,
        T::Archived: fmt::Debug,
    {
        assert!(node.len() >= 3, "pages should always contain at least 3 entries");
        // split the non-root interior by allocating a new interior and splitting the contents
        // then we insert the separator key and the new page index into the parent
        let new = self.pool.alloc().await?;
        let mut new_data = new.write().await;
        let new_page_idx = new_data.page_idx();
        let mut new_node = N::initialize(new_data.get_mut());

        N::split_left_into(&mut node, new_page_idx, &mut new_node, node_page_idx);

        let sep = new_node.low_key().unwrap();
        self.insert_interior(parents, sep, new_page_idx).await?;

        // insert the new separator key and child index into the parent
        (if &key < sep { node.insert(key, value) } else { new_node.insert(key, value) })
            .expect("split interior should not be full");
        Ok(())
    }

    async fn split_root<N, T>(
        &self,
        mut root: N::ViewMut<'_>,
        key: K::Archived,
        value: T::Archived,
    ) -> Result<()>
    where
        N: NodeMut<K, T>,
        T: Archive + fmt::Debug,
        T::Archived: fmt::Debug,
    {
        let left_page = self.pool.alloc().await?;
        let mut left_data = left_page.write().await;

        let right_data = self.pool.alloc().await?;
        let mut right_data = right_data.write().await;

        assert!(root.len() >= 3, "pages should always contain at least 3 entries");
        let left_page_idx = left_data.page_idx();
        let right_page_idx = right_data.page_idx();
        let mut left_child = N::initialize(left_data.get_mut());
        let mut right_child = N::initialize(right_data.get_mut());

        N::split_root_into(
            &mut root,
            left_page_idx,
            &mut left_child,
            right_page_idx,
            &mut right_child,
        );

        let sep = right_child.low_key().unwrap().clone();

        (if key < sep { left_child } else { right_child })
            .insert(key, value)
            .expect("split child should not be full");

        // reinitialize the root to an interior root node and add the two children
        let mut root = root.reinitialize_as_root_interior();
        root.insert(K::MIN, left_page_idx).expect("new root should not be full");
        root.insert(sep, right_page_idx).expect("new root should not be full");
        Ok(())
    }
}

// hack to generate "negative infinity" low keys for L&Y trees for left-most nodes
pub trait Min: Archive {
    const MIN: Self::Archived;
}

impl Min for u32 {
    const MIN: Self::Archived = rkyv::rend::BigEndian::<u32>::new(0);
}
