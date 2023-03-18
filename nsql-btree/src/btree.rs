use std::fmt;
use std::marker::PhantomData;

use async_recursion::async_recursion;
use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;

use crate::node::Flags;
use crate::page::{PageFull, PageView, PageViewMut, PageViewMutKind};
use crate::{Result, Rkyv};

/// A B+ tree
pub struct BTree<K, V> {
    pool: BufferPool,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Min + Ord + Send + Sync + Rkyv + fmt::Debug,
    K::Archived: fmt::Debug + Ord + PartialOrd<K>,
    V: Rkyv + Eq + Clone + fmt::Debug,
{
    #[inline]
    pub async fn init(pool: BufferPool) -> Result<Self> {
        let handle = pool.alloc().await?;
        let page = handle.page();
        let mut data = page.data_mut().await;

        PageViewMut::<K, V>::init_root_leaf(&mut data).await?;
        let root_idx = page.idx();
        Ok(Self { pool, root_idx, marker: PhantomData })
    }

    #[inline]
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_bytes = key.serialize();
        self.search_node(self.root_idx, K::archived(&key_bytes)).await
    }

    #[inline]
    pub async fn insert(&self, key: &K, value: &V) -> Result<()> {
        let key_bytes = key.serialize();
        let value_bytes = value.serialize();
        self.insert_archived(K::archived(&key_bytes), V::archived(&value_bytes)).await
    }

    #[async_recursion(?Send)]
    async fn search_node(&self, idx: PageIndex, key: &K::Archived) -> Result<Option<V>> {
        let handle = self.pool.load(idx).await?;
        let data = handle.page().data().await;
        let node = unsafe { PageView::<K, V>::create(&data).await? };
        match node {
            PageView::Leaf(leaf) => Ok(leaf.get(key).await?.map(|v| nsql_rkyv::deserialize(v))),
            PageView::Interior(interior) => {
                let child_idx = interior.search(key).await?;
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
        let data = handle.page().data().await;
        let node = unsafe { PageView::<K, V>::create(&data).await? };
        match node {
            PageView::Leaf(_) => Ok(idx),
            PageView::Interior(interior) => {
                stack.push(idx);
                let child_idx = interior.search(key).await?;
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
        let mut leaf_data = leaf_handle.page().data_mut().await;
        let mut view = unsafe { PageViewMut::<K, V>::create(&mut leaf_data).await? };
        let leaf = match &mut view.kind {
            PageViewMutKind::Interior(_) => unreachable!("should have been passed a leaf page"),
            PageViewMutKind::Leaf(leaf) => leaf,
        };

        match leaf.insert(key, value).await? {
            Ok(value) => Ok(value),
            Err(PageFull) => {
                if view.header.flags.contains(Flags::IS_ROOT) {
                    let left_page = self.pool.alloc().await?;
                    let mut left_data = left_page.page().data_mut().await;
                    let mut left_child = PageViewMut::<K, V>::init_leaf(&mut left_data).await?;

                    let right_page = self.pool.alloc().await?;
                    let mut right_data = right_page.page().data_mut().await;
                    let mut right_child = PageViewMut::<K, V>::init_leaf(&mut right_data).await?;

                    leaf.split_root_into(&mut left_child, &mut right_child).await?;

                    // reinitialize the root to an interior node and add the two children
                    let mut root = PageViewMut::<K, V>::init_root_interior(&mut leaf_data).await?;

                    // use the first key in the right child as the separator to insert into the parent
                    let sep = right_child.low_key()?;

                    root.insert_initial(K::MIN, left_page.page_idx(), sep, right_page.page_idx())
                        .await?
                        .expect("new root should not be full");

                    // FIXME check correctness of condition
                    (if key < sep { left_child } else { right_child })
                        .insert(key, value)
                        .await?
                        .expect("split child should not be full");

                    Ok(())
                } else {
                    // split the non-root leaf by allocating a new leaf and splitting the contents
                    // then we insert the separator key and the new page index into the parent
                    let new_page = self.pool.alloc().await?;
                    let mut new_data = new_page.page().data_mut().await;
                    let mut new_leaf = PageViewMut::<K, V>::init_leaf(&mut new_data).await?;

                    let sep = leaf.split_into(&mut new_leaf).await?;
                    self.insert_interior(parents, sep, new_page.page_idx()).await?;

                    // FIXME check condition correctness
                    (if key < sep { leaf } else { &mut new_leaf })
                        .insert(key, value)
                        .await?
                        .expect("split leaf should not be full");

                    Ok(())
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
        let mut parent_data = parent_page.page().data_mut().await;
        let parent_view = unsafe { PageViewMut::<K, V>::create(&mut parent_data).await? };
        let mut parent = match parent_view.kind {
            PageViewMutKind::Interior(interior) => interior,
            PageViewMutKind::Leaf(_) => {
                unreachable!("parent should be interior page")
            }
        };

        match parent.insert(key, child_idx).await? {
            Ok(()) => {}
            Err(PageFull) => {
                if parent_view.header.flags.contains(Flags::IS_ROOT) {
                    let left_page = self.pool.alloc().await?;
                    let mut left_data = left_page.page().data_mut().await;
                    let mut left_child = PageViewMut::<K, V>::init_interior(&mut left_data).await?;

                    let right_page = self.pool.alloc().await?;
                    let mut right_data = right_page.page().data_mut().await;
                    let mut right_child =
                        PageViewMut::<K, V>::init_interior(&mut right_data).await?;

                    parent
                        .split_root_into(left_page.page_idx(), &mut left_child, &mut right_child)
                        .await?;

                    // reinitialize the root to an interior node and add the two children
                    let mut root =
                        PageViewMut::<K, V>::init_root_interior(&mut parent_data).await?;

                    let sep = right_child.low_key()?;

                    root.insert_initial(K::MIN, left_page.page_idx(), sep, right_page.page_idx())
                        .await?
                        .expect("new root should not be full");

                    (if key < sep { left_child } else { right_child })
                        .insert(key, child_idx)
                        .await?
                        .expect("split child should not be full");
                } else {
                    // split the non-root interior by allocating a new interior and splitting the contents
                    // then we insert the separator key and the new page index into the parent
                    let new_page = self.pool.alloc().await?;
                    let mut new_data = new_page.page().data_mut().await;
                    let mut new_interior =
                        PageViewMut::<K, V>::init_interior(&mut new_data).await?;

                    let sep = parent.split_into(&mut new_interior).await?;

                    // insert the new separator key and child index into the parent
                    (if sep > key { parent } else { new_interior })
                        .insert(key, child_idx)
                        .await?
                        .expect("split interior should not be full");

                    self.insert_interior(parents, sep, new_page.page_idx()).await?;
                }
            }
        }

        Ok(())
    }
}

// hack to generate "negative infinity" low keys for L&Y trees for left-most nodes
pub trait Min: Rkyv {
    const MIN: &'static Self::Archived;
}

impl Min for u32 {
    const MIN: &'static Self::Archived = &rkyv::rend::BigEndian::<u32>::new(0);
}
