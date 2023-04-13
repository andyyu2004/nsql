mod fsm_page;

use std::sync::Arc;

use nsql_btree::{BTree, Min};
use nsql_buffer::{BufferHandle, Pool};
use nsql_pager::PageIndex;
use rkyv::{Archive, Deserialize, Serialize};

use crate::table_storage::HeapTuple;

#[derive(Debug, Archive, Serialize)]
struct FsmMeta {
    tree_root_page: PageIndex,
    itree_root_page: PageIndex,
    unique: u64,
}

pub struct FreeSpaceMap {
    meta_page: BufferHandle,
    tree: BTree<PageIndex, Key>,
    itree: BTree<Key, PageIndex>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
#[archive_attr(derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy))]
#[archive(compare(PartialOrd, PartialEq))]
struct Key {
    size: u16,
    unique: u64,
}

impl Min for Key {
    const MIN: Self = Self { size: 0, unique: 0 };
}

impl PartialEq<u16> for ArchivedKey {
    fn eq(&self, other: &u16) -> bool {
        self.size == *other
    }
}

impl PartialOrd<u16> for ArchivedKey {
    fn partial_cmp(&self, other: &u16) -> Option<std::cmp::Ordering> {
        self.size.partial_cmp(other)
    }
}

impl FreeSpaceMap {
    pub async fn initialize(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        let meta_page = pool.alloc().await?;
        let tree = BTree::initialize(Arc::clone(&pool)).await?;
        let itree = BTree::initialize(pool).await?;
        let meta = FsmMeta {
            tree_root_page: tree.root_page(),
            itree_root_page: itree.root_page(),
            unique: 0,
        };

        let mut guard = meta_page.write();
        let bytes = nsql_rkyv::to_bytes(&meta);
        guard[..bytes.len()].copy_from_slice(&bytes);
        drop(guard);

        Ok(Self { meta_page, tree, itree })
    }

    pub async fn load(pool: Arc<dyn Pool>, fsm_root_idx: PageIndex) -> nsql_buffer::Result<Self> {
        let meta_page = pool.load(fsm_root_idx).await?;
        let guard = meta_page.read();
        let (meta_bytes, _) = guard.split_array_ref();
        let meta = unsafe { nsql_rkyv::archived_root::<FsmMeta>(meta_bytes) };
        let tree = BTree::load(Arc::clone(&pool), meta.tree_root_page.into()).await?;
        let itree = BTree::load(pool, meta.itree_root_page.into()).await?;
        drop(guard);
        Ok(Self { tree, itree, meta_page })
    }

    /// find a page with at least `required_size` free space, returning the page index
    // should the interface to the fsm be find and release instead of find and update?
    pub async fn find(&self, required_size: u16) -> nsql_buffer::Result<Option<PageIndex>> {
        assert!(required_size > 0);
        assert!(required_size <= HeapTuple::MAX_SIZE);
        self.itree.find_min(&required_size).await
    }

    /// update the free space map with the new free space on the page
    pub async fn update(&self, page_idx: PageIndex, size: u16) -> nsql_buffer::Result<()> {
        // FIXME these assertions aren't really safe under concurrent workloads
        // FIXME these operations should be transactional
        assert!(size <= HeapTuple::MAX_SIZE);
        let key = Key { size, unique: self.next_unique() };
        let prior_free_space = self.tree.insert(&page_idx, &key).await?;
        if let Some(sz) = prior_free_space {
            assert_eq!(self.itree.remove(&sz).await?, Some(page_idx));
        }
        self.itree.insert(&key, &page_idx).await?;
        Ok(())
    }

    fn next_unique(&self) -> u64 {
        let mut guard = self.meta_page.write();
        let (meta_bytes, _) = guard.split_array_mut();
        let mut meta = unsafe { nsql_rkyv::archived_root_mut::<FsmMeta>(meta_bytes) };
        meta.unique += 1;
        meta.unique.into()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fsm_page_tests;
