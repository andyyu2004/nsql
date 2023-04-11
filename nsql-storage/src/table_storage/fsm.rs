mod fsm_page;

use std::sync::Arc;

use nsql_btree::{BTree, Min};
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use rkyv::{Archive, Deserialize, Serialize};

use crate::table_storage::HeapTuple;

pub struct Fsm {
    tree: BTree<PageIndex, u16>,
    inverse_tree: BTree<u16, PageIndex>,
}

pub struct FreeSpaceMap {
    tree: BTree<PageIndex, u16>,
}

impl FreeSpaceMap {
    pub async fn initialize(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        Ok(Self { tree: BTree::initialize(pool).await? })
    }

    pub async fn load(pool: Arc<dyn Pool>, root_idx: PageIndex) -> nsql_buffer::Result<Self> {
        Ok(Self { tree: BTree::load(pool, root_idx).await? })
    }

    /// find a page with at least `required_size` free space, returning the page index
    pub async fn find(&self, required_size: u16) -> nsql_buffer::Result<Option<PageIndex>> {
        assert!(required_size > 0);
        assert!(required_size <= HeapTuple::MAX_SIZE);
        todo!();
    }

    /// update the free space map with the new free space on the page
    pub async fn update(&self, page_idx: PageIndex, free_space: u16) -> nsql_buffer::Result<()> {
        assert!(free_space <= HeapTuple::MAX_SIZE);
        self.tree.insert(&page_idx, &free_space).await?;
        todo!()
    }

    async fn update_rec(
        &self,
        _fsm_page_idx: PageIndex,
        _level: u16,
        _page: PageIndex,
        _free_space: u16,
    ) -> nsql_buffer::Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fsm_page_tests;
