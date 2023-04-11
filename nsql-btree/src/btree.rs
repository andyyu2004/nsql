#![deny(clippy::await_holding_lock)]

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_recursion::async_recursion;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_rkyv::DefaultSerializer;
use rkyv::{rend, Archive, Deserialize, Serialize};
use tokio::runtime::Handle;

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

#[derive(Debug)]
pub(crate) struct ConcurrentSplit;

impl<K, V> BTree<K, V>
where
    K: Min + Archive + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    K::Archived:
        Deserialize<K, rkyv::Infallible> + PartialOrd<K> + fmt::Debug + Clone + Ord + Send + Sync,
    V: Archive + Serialize<DefaultSerializer> + fmt::Debug + Send + Sync + 'static,
    V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug + Send + Sync,
{
    #[inline]
    pub async fn initialize(pool: Arc<dyn Pool>) -> Result<Self> {
        let handle = pool.alloc().await?;
        let mut guard = handle.write();

        let _root = LeafPageViewMut::<K, V>::initialize_root(&mut guard);

        let root_idx = handle.page_idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    pub async fn load(pool: Arc<dyn Pool>, root_idx: PageIndex) -> Result<Self> {
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    #[tracing::instrument(skip(key))]
    // we instrument the key with `search_node`, we just want `self` here
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.search_node(self.root_idx, key).await
    }

    /// Insert a key-value pair into the tree returning the previous value if it existed
    #[inline]
    #[tracing::instrument]
    pub async fn insert(&self, key: &K, value: &V) -> Result<Option<V>> {
        const MAX_ATTEMPTS: usize = 100;
        for _ in 0..MAX_ATTEMPTS {
            let (leaf_idx, parents) = self.find_leaf_page_idx(key).await?;
            match self.insert_leaf(leaf_idx, &parents, key, value).await? {
                Ok(prev) => return Ok(prev),
                Err(ConcurrentSplit) => continue,
            }
        }

        // FIXME need to handle this properly rather than braindead retrying
        panic!("failed to insert after {MAX_ATTEMPTS} attempts")
    }

    #[async_recursion]
    #[tracing::instrument(skip(self))]
    async fn search_node(&self, idx: PageIndex, key: &K) -> Result<Option<V>> {
        let next_idx = {
            let handle = self.pool.load(idx).await?;
            let guard = handle.read();
            let node = unsafe { PageView::<K, V>::view(&guard)? };
            match node {
                PageView::Leaf(leaf) => match leaf.get(key) {
                    Ok(value) => return Ok(value.map(nsql_rkyv::deserialize)),
                    Err(ConcurrentSplit) => match leaf.right_link() {
                        Some(idx) => idx,
                        None => return Ok(None),
                    },
                },
                PageView::Interior(interior) => {
                    let child_idx = interior.search(key);
                    assert_ne!(
                        child_idx, idx,
                        "child index should not be the same as parent index"
                    );
                    child_idx
                }
            }
        };

        self.search_node(next_idx, key).await
    }

    /// Find the leaf page that should contain the given key, and return the index of that page and
    /// stack of parent page indices.
    async fn find_leaf_page_idx(&self, key: &K) -> Result<(PageIndex, Vec<PageIndex>)> {
        let mut parents = vec![];
        let leaf_idx = self.find_leaf_page_idx_rec(self.root_idx, key, &mut parents).await?;
        Ok((leaf_idx, parents))
    }

    #[async_recursion]
    async fn find_leaf_page_idx_rec(
        &self,
        idx: PageIndex,
        key: &K,
        stack: &mut Vec<PageIndex>,
    ) -> Result<PageIndex> {
        tracing::trace!(?idx, ?key, "find_leaf_page_idx_rec");
        let child_idx = {
            let handle = self.pool.load(idx).await?;
            let guard = handle.read();
            let node = unsafe { PageView::<K, V>::view(&guard)? };
            tracing::trace!(?idx, ?key, "found leaf page");
            match node {
                PageView::Leaf(leaf) => {
                    tracing::trace!(?idx, ?key, "found leaf page");
                    match leaf.high_key().as_ref() {
                        Some(high_key) if high_key < key => {
                            tracing::debug!(
                                ?idx,
                                ?high_key,
                                ?key,
                                "detected concurrent split when searching for leaf page to insert into"
                            );
                            leaf.right_link()
                                .expect("did not have a right link when high key was set")
                        }
                        _ => return Ok(idx),
                    }
                }
                PageView::Interior(interior) => {
                    stack.push(idx);
                    interior.search(key)
                }
            }
        };

        self.find_leaf_page_idx_rec(child_idx, key, stack).await
    }

    #[cold]
    fn concurrent_root_split(interior: InteriorPageViewMut<'_, K>) -> ConcurrentSplit {
        assert!(
            interior.is_root(),
            "BUG: a non-root node changed from being a leaf to an interior node"
        );

        tracing::debug!("detected concurrent root split");

        ConcurrentSplit
    }

    #[tracing::instrument(skip(self, key, value))]
    #[inline]
    async fn insert_leaf(
        &self,
        leaf_page_idx: PageIndex,
        parents: &[PageIndex],
        key: &K,
        value: &V,
    ) -> Result<Result<Option<V>, ConcurrentSplit>> {
        let (_prev, sep, new_page_idx) = {
            let leaf_handle = self.pool.load(leaf_page_idx).await?;
            let mut leaf_guard = leaf_handle.write();
            let view = unsafe { PageViewMut::<K, V>::view_mut(&mut leaf_guard)? };

            let mut leaf = match view {
                PageViewMut::Interior(interior) => {
                    // We expected `leaf_page_idx` to be a leaf page, but it may have been concurrently
                    // changed to an interior page due to a root split.
                    // We just return a special error and allow the caller to retry the operation.
                    return Ok(Err(Self::concurrent_root_split(interior)));
                }
                PageViewMut::Leaf(leaf) => leaf,
            };

            let prev = leaf.insert(key, value);
            match prev {
                Ok(Ok(value)) => return Ok(Ok(value)),
                Ok(Err(PageFull)) => {
                    if leaf.page_header().flags.contains(Flags::IS_ROOT) {
                        cov_mark::hit!(root_leaf_split);
                        return self
                            .split_root_and_insert::<LeafPageViewMut<'_, K, V>, _>(leaf, key, value)
                            .map(Ok);
                    } else {
                        cov_mark::hit!(non_root_leaf_split);
                        let right_handle = leaf
                            .right_link()
                            .map(|right_idx| {
                                tokio::task::block_in_place(|| {
                                    Handle::current().block_on(self.pool.load(right_idx))
                                })
                            })
                            .transpose()?;
                        let right_guard =
                            right_handle.as_ref().map(|right_page| right_page.write());

                        self.split_non_root::<LeafPageViewMut<'_, K, V>, _>(
                            leaf_guard,
                            right_guard,
                            key,
                            value,
                        )?
                    }
                }
                Err(ConcurrentSplit) => return Ok(Err(ConcurrentSplit)),
            }
        };

        self.insert_interior(parents, &sep, new_page_idx).await?;
        Ok(Ok(_prev))
    }

    #[async_recursion]
    async fn insert_interior(
        &self,
        parents: &[PageIndex],
        sep: &K,
        child_idx: PageIndex,
    ) -> Result<()> {
        let (&parent_idx, parents) =
            parents.split_last().expect("non-root leaf must have at least one parents");

        let (_prev, sep, new_node_page_idx) = {
            let parent_page = self.pool.load(parent_idx).await?;

            let mut parent_guard = parent_page.write();
            let parent_view = unsafe { PageViewMut::<K, V>::view_mut(&mut parent_guard)? };
            let mut parent = match parent_view {
                PageViewMut::Interior(interior) => interior,
                PageViewMut::Leaf(_) => {
                    unreachable!("parent should be interior page")
                }
            };

            match parent.insert(sep, &child_idx) {
                Ok(Ok(None)) => return Ok(()),
                Ok(Ok(Some(prev))) => todo!(
                    "duplicate key `{sep:?}` in interior node, already pointed to `{prev}`, trying to insert `{child_idx}`"
                ),
                Ok(Err(PageFull)) => {
                    if parent.page_header().flags.contains(Flags::IS_ROOT) {
                        cov_mark::hit!(root_interior_split);
                        self.split_root_and_insert::<InteriorPageViewMut<'_, K>, _>(
                            parent, sep, &child_idx,
                        )?;
                        return Ok(());
                    } else {
                        cov_mark::hit!(non_root_interior_split);

                        let right_handle = parent
                            .right_link()
                            .map(|right_idx| {
                                tokio::task::block_in_place(|| {
                                    Handle::current().block_on(self.pool.load(right_idx))
                                })
                            })
                            .transpose()?;
                        let right_guard =
                            right_handle.as_ref().map(|right_page| right_page.write());

                        self.split_non_root::<InteriorPageViewMut<'_, K>, _>(
                            parent_guard,
                            right_guard,
                            sep,
                            &child_idx,
                        )?
                    }
                }
                Err(ConcurrentSplit) => todo!(),
            }
        };

        self.insert_interior(parents, &sep, new_node_page_idx).await?;
        Ok(())
    }

    fn split_non_root<N, T>(
        &self,
        mut old_guard: nsql_pager::PageWriteGuard<'_>,
        mut prev_right_guard: Option<nsql_pager::PageWriteGuard<'_>>,
        key: &K,
        value: &T,
    ) -> Result<(Option<T>, K, PageIndex)>
    where
        N: NodeMut<K, T>,
        T: Archive + Serialize<DefaultSerializer> + fmt::Debug + 'static,
        T::Archived: Deserialize<T, rkyv::Infallible> + fmt::Debug,
    {
        let old_node_page_idx = old_guard.page_idx();
        let mut old_node = unsafe { N::view_mut(&mut old_guard) }?;

        tracing::debug!(kind = %std::any::type_name::<N>(), "splitting non-root");
        // split the non-root interior by allocating a new interior and splitting the contents
        // then we insert the separator key and the new page index into the parent

        // HACK avoiding making this function async due to mutex guard send issues
        tracing::debug!("allocating new page for split");
        let new_page =
            tokio::task::block_in_place(|| Handle::current().block_on(self.pool.alloc()))?;
        tracing::debug!(new_page_idx = ?new_page.page_idx(), "allocated new page for split");
        let mut new_guard = new_page.page().write();
        let new_node_page_idx = new_guard.page_idx();
        let mut new_node = N::initialize(&mut new_guard);

        let mut prev_right_page =
            prev_right_guard.as_mut().map(|guard| unsafe { N::view_mut(guard) }).transpose()?;

        N::split(
            &mut old_node,
            old_node_page_idx,
            &mut new_node,
            new_node_page_idx,
            prev_right_page.as_mut(),
        );

        // a separator between the new left node and the right node is the min key of the right node
        let sep = new_node.min_key().unwrap();
        let sep_key = nsql_rkyv::deserialize(sep);
        let prev =
            if sep > key { old_node.insert(key, value) } else { new_node.insert(key, value) };

        let prev = match prev {
            Ok(Ok(prev)) => prev,
            Ok(Err(PageFull)) => unreachable!("split node should have space for at least one key"),
            Err(ConcurrentSplit) => todo!(),
        };

        Ok((prev, sep_key, new_node_page_idx))
    }

    #[tracing::instrument(skip(root))]
    fn split_root_and_insert<N, T>(
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
        tracing::info!(kind = %std::any::type_name::<N>(), "splitting root");

        assert!(root.len() >= 3, "root that requires a split should contain at least 3 entries");
        // HACK to avoid making the function async and running into mutex guard not being send issues
        let (left_page, right_page) = tokio::task::block_in_place(|| {
            Handle::current().block_on(async {
                let (a, b) = tokio::join!(self.pool.alloc(), self.pool.alloc());
                Ok::<_, nsql_pager::Error>((a?, b?))
            })
        })?;

        let mut left_guard = left_page.write();
        let mut right_guard = right_page.write();

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

        let sep = right_child.min_key().unwrap();

        // reinitialize the root to an interior root node and add the two children
        let mut root = root.reinitialize_as_root_interior();
        root.insert(&K::MIN, &left_page_idx)
            .expect("root is locked and shouldn't see concurrent split")
            .expect("new root should not be full");
        // FIXME ideally we don't have to deserialize sep
        root.insert(&nsql_rkyv::deserialize(sep), &right_page_idx)
            .expect("root is locked and shouldn't see concurrent split")
            .expect("new root should not be full");

        let prev = (if sep > key { left_child } else { right_child })
            .insert(key, value)
            .expect("root is locked and shouldn't see concurrent split")
            .expect("split nodes should not be full");

        Ok(prev)
    }
}

// hack to make implementation of btree easier for now. Shouldn't be necessary
pub trait Min {
    const MIN: Self;
}

impl<T, U> Min for (T, U)
where
    T: Min,
    U: Min,
{
    const MIN: Self = (T::MIN, U::MIN);
}

impl Min for PageIndex {
    const MIN: Self = PageIndex::ZERO;
}

impl Min for rend::BigEndian<u16> {
    const MIN: Self = Self::new(0);
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

impl_min_numeric!(i8, i16, i32, i64, u8, u16, u32, u64);
