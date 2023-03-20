use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_recursion::async_recursion;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_rkyv::DefaultSerializer;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::{
    Flags, InteriorPageViewMut, LeafPageViewMut, Node, NodeMut, PageFull, PageView, PageViewMut,
};
use crate::Result;

/// A B+ tree
pub struct BTree<K, V> {
    pool: Arc<dyn Pool>,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Min + Ord + Send + Sync + Archive + Serialize<DefaultSerializer> + fmt::Debug,
    K::Archived: PartialOrd<K> + Clone + fmt::Debug + Ord,
    V: Archive + Serialize<DefaultSerializer> + Eq + Clone + fmt::Debug,
    V::Archived: Clone + Deserialize<V, rkyv::Infallible> + fmt::Debug,
{
    #[inline]
    pub async fn init(pool: Arc<dyn Pool>) -> Result<Self> {
        let handle = pool.alloc().await?;
        let page = handle.page();
        let mut data = page.data_mut().await;

        LeafPageViewMut::<K, V>::init_root(&mut data);
        let root_idx = page.idx();
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
        let data = handle.page().data().await;
        let node = unsafe { PageView::<K, V>::create(&data).await? };
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
        let data = handle.page().data().await;
        let node = unsafe { PageView::<K, V>::create(&data).await? };
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
        let mut leaf_data = leaf_handle.page().data_mut().await;
        let mut view = unsafe { PageViewMut::<K, V>::create(&mut leaf_data).await? };
        let leaf = match &mut view {
            PageViewMut::Interior(_) => unreachable!("should have been passed a leaf page"),
            PageViewMut::Leaf(leaf) => leaf,
        };

        match leaf.insert(key.clone(), value.clone()) {
            Ok(value) => Ok(value),
            Err(PageFull) => {
                if leaf.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_leaf_split);
                    let left_page = self.pool.alloc().await?;
                    let mut left_data = left_page.page().data_mut().await;
                    let mut left_child = LeafPageViewMut::<K, V>::init(&mut left_data);

                    let right_page = self.pool.alloc().await?;
                    let mut right_data = right_page.page().data_mut().await;
                    let mut right_child = LeafPageViewMut::<K, V>::init(&mut right_data);

                    leaf.split_root_into(left_page.page_idx(), &mut left_child, &mut right_child);

                    // use the first key in the right child as the separator to insert into the parent
                    let sep = right_child.low_key().clone();

                    // FIXME check correctness of condition
                    (if key < &sep { left_child } else { right_child })
                        .insert(key.clone(), value.clone())
                        .expect("split child should not be full");

                    // reinitialize the root to an interior node and add the two children
                    let mut root = InteriorPageViewMut::<K>::init_root(&mut leaf_data);
                    root.insert_initial(K::MIN, left_page.page_idx(), sep, right_page.page_idx())
                        .expect("new root should not be full");

                    Ok(())
                } else {
                    cov_mark::hit!(non_root_leaf_split);
                    // split the non-root leaf by allocating a new leaf and splitting the contents
                    // then we insert the separator key and the new page index into the parent
                    let new_page = self.pool.alloc().await?;
                    let mut new_data = new_page.page().data_mut().await;
                    let mut new_leaf = LeafPageViewMut::<K, V>::init(&mut new_data);

                    leaf.split_into(&mut new_leaf);
                    let sep = new_leaf.low_key();

                    self.insert_interior(parents, sep, new_page.page_idx()).await?;

                    // FIXME check condition correctness
                    (if key < sep { leaf } else { &mut new_leaf })
                        .insert(key.clone(), value.clone())
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
        let mut parent = match parent_view {
            PageViewMut::Interior(interior) => interior,
            PageViewMut::Leaf(_) => {
                unreachable!("parent should be interior page")
            }
        };

        match parent.insert(key.clone(), child_idx).await? {
            Ok(()) => {}
            Err(PageFull) => {
                if parent.page_header().flags.contains(Flags::IS_ROOT) {
                    cov_mark::hit!(root_interior_split);
                    let left_page = self.pool.alloc().await?;
                    let mut left_data = left_page.page().data_mut().await;
                    let mut left_child = InteriorPageViewMut::<K>::init(&mut left_data);

                    let right_page = self.pool.alloc().await?;
                    let mut right_data = right_page.page().data_mut().await;
                    let mut right_child = InteriorPageViewMut::<K>::init(&mut right_data);

                    parent.split_root_into(left_page.page_idx(), &mut left_child, &mut right_child);

                    let sep = right_child.low_key().clone();

                    (if key < &sep { left_child } else { right_child })
                        .insert(key.clone(), child_idx)
                        .await?
                        .expect("split child should not be full");

                    // reinitialize the root to an interior root node and add the two children
                    let mut root = InteriorPageViewMut::<K>::init_root(&mut parent_data);
                    root.insert_initial(K::MIN, left_page.page_idx(), sep, right_page.page_idx())
                        .expect("new root should not be full");
                } else {
                    cov_mark::hit!(non_root_interior_split);
                    // split the non-root interior by allocating a new interior and splitting the contents
                    // then we insert the separator key and the new page index into the parent
                    let new_page = self.pool.alloc().await?;
                    let mut new_data = new_page.page().data_mut().await;
                    let mut new_interior = InteriorPageViewMut::<K>::init(&mut new_data);

                    parent.split_into(&mut new_interior);
                    let sep = new_interior.low_key();
                    self.insert_interior(parents, sep, new_page.page_idx()).await?;

                    // insert the new separator key and child index into the parent
                    (if key < sep { parent } else { new_interior })
                        .insert(key.clone(), child_idx)
                        .await?
                        .expect("split interior should not be full");
                }
            }
        }

        Ok(())
    }

    // async fn split_root(&mut self) -> nsql_serde::Result<()> {
    //     let left_page = self.pool.alloc().await?;
    //     let mut left_data = left_page.page().data_mut().await;
    //     let mut left_child = PageViewMut::<K, V>::init_leaf(&mut left_data).await?;

    //     let right_page = self.pool.alloc().await?;
    //     let mut right_data = right_page.page().data_mut().await;
    //     let mut right_child = PageViewMut::<K, V>::init_leaf(&mut right_data).await?;

    //     leaf.split_root_into(&mut left_child, &mut right_child);
    //     Ok(())
    // }
}

// hack to generate "negative infinity" low keys for L&Y trees for left-most nodes
pub trait Min: Archive {
    const MIN: Self::Archived;
}

impl Min for u32 {
    const MIN: Self::Archived = rkyv::rend::BigEndian::<u32>::new(0);
}
