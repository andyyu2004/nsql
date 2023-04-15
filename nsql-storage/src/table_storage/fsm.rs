use std::sync::Arc;
use std::{fmt, io};

use nsql_btree::{BTree, Min};
use nsql_buffer::{BufferHandle, Pool};
use nsql_pager::{PageIndex, PageWriteGuard};
use rkyv::{Archive, Deserialize, Serialize};

const FSM_MAGIC: [u8; 4] = *b"FSMM";

#[derive(Debug, Archive, Serialize)]
struct FsmMeta {
    magic: [u8; 4],
    tree_root_page: PageIndex,
    itree_root_page: PageIndex,
    unique: u64,
}

pub struct FreeSpaceMap {
    meta_page_idx: PageIndex,
    meta_page: BufferHandle,
    tree: BTree<PageIndex, Key>,
    itree: BTree<Key, PageIndex>,
}

impl fmt::Debug for FreeSpaceMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FreeSpaceMap")
            .field("meta_page_idx", &self.meta_page_idx)
            .finish_non_exhaustive()
    }
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

impl FreeSpaceMap {
    pub async fn initialize(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        let meta_page = pool.alloc().await?;
        let tree = BTree::initialize(Arc::clone(&pool)).await?;
        let itree = BTree::initialize(pool).await?;
        let meta = FsmMeta {
            tree_root_page: tree.root_page(),
            itree_root_page: itree.root_page(),
            unique: 0,
            magic: FSM_MAGIC,
        };

        let mut guard = meta_page.write();
        let bytes = nsql_rkyv::to_bytes(&meta);
        guard[..bytes.len()].copy_from_slice(&bytes);
        drop(guard);

        Ok(Self { meta_page_idx: meta_page.page_idx(), meta_page, tree, itree })
    }

    #[tracing::instrument(skip(pool))]
    pub async fn load(pool: Arc<dyn Pool>, meta_page_idx: PageIndex) -> nsql_buffer::Result<Self> {
        let meta_page = pool.load(meta_page_idx).await?;
        let guard = meta_page.read();
        let (meta_bytes, _) = guard.split_array_ref();
        let meta = unsafe { nsql_rkyv::archived_root::<FsmMeta>(meta_bytes) };
        if meta.magic != FSM_MAGIC {
            Err(io::Error::new(io::ErrorKind::InvalidData, "invalid fsm magic"))?;
        }

        let tree = BTree::load(Arc::clone(&pool), meta.tree_root_page.into()).await?;
        let itree = BTree::load(pool, meta.itree_root_page.into()).await?;
        drop(guard);
        Ok(Self { meta_page_idx, tree, itree, meta_page })
    }

    /// find a page with at least `required_size` free space, returning the page index
    // should the interface to the fsm be find and release instead of find and update?
    #[tracing::instrument]
    pub async fn find(&self, required_size: u16) -> nsql_buffer::Result<Option<PageIndex>> {
        assert!(required_size > 0);
        self.itree.find_min(&Key { size: required_size, unique: Min::MIN }).await
    }

    /// update the free space map with the new free space on the page
    /// Takes a `PageWriteGuard` as proof to ensure that the free space for the page is not concurrently modified
    #[tracing::instrument]
    pub async fn update(&self, guard: &PageWriteGuard<'_>, size: u16) -> nsql_buffer::Result<()> {
        tracing::trace!(size, "updating free space map");
        let page_idx = guard.page_idx();
        // FIXME these assertions aren't really safe under concurrent workloads
        // FIXME these operations should be transactional
        let new_key = Key { size, unique: self.next_unique() };
        let prior_key = self.tree.insert(&page_idx, &new_key).await?;
        assert!(
            self.itree.insert(&new_key, &page_idx).await?.is_none(),
            "the new_key should be unique"
        );

        if let Some(prior_key) = prior_key {
            tracing::trace!(?prior_key, "removing prior key itree");
            assert_eq!(
                self.itree.remove(&prior_key).await?,
                Some(page_idx),
                "the prior key should exist"
            );
            debug_assert!(
                self.itree
                    .find_min(&Key { size: prior_key.size, unique: 0 })
                    .await?
                    .map(|idx| idx != page_idx || size >= prior_key.size)
                    .unwrap_or(true),
                "the removed page index should not be returned unless the new size is >= than the prior size"
            );
        }
        Ok(())
    }

    #[inline]
    pub fn meta_page_idx(&self) -> PageIndex {
        self.meta_page_idx
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
