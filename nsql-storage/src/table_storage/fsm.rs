mod fsm_page;

use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_serde::{StreamDeserialize, StreamSerialize};

use self::fsm_page::FsmPage;
use crate::table_storage::HeapTuple;

pub struct FreeSpaceMap {
    pool: Arc<dyn Pool>,
    meta: FreeSpaceMapMeta,
}

#[derive(Debug, StreamSerialize, StreamDeserialize)]
pub struct FreeSpaceMapMeta {
    root_fsm_page: PageIndex,
    // pages: Vec<PageIndex>,
}

impl FreeSpaceMap {
    pub async fn create(pool: Arc<dyn Pool>) -> nsql_buffer::Result<Self> {
        let root_fsm_page = pool.pager().alloc_page().await?;
        let meta = FreeSpaceMapMeta { root_fsm_page };
        Ok(Self { pool, meta })
    }

    pub fn load(pool: Arc<dyn Pool>, meta: FreeSpaceMapMeta) -> Self {
        Self { pool, meta }
    }

    /// find a page with at least `required_size` free space, returning the page index
    pub async fn find(&self, required_size: u16) -> nsql_buffer::Result<Option<PageIndex>> {
        assert!(required_size > 0);
        assert!(required_size <= HeapTuple::MAX_SIZE);

        self.find_rec(self.meta.root_fsm_page, required_size, 0).await
    }

    async fn find_rec(
        &self,
        _fsm_page_idx: PageIndex,
        _required_size: u16,
        _level: usize,
    ) -> nsql_buffer::Result<Option<PageIndex>> {
        let buffer = self.pool.load(self.meta.root_fsm_page).await?;
        let mut data = buffer.page().write();
        let _fsm_page = FsmPage::from_bytes_mut(&mut data);
        // let _offset = fsm_page.find(required_size);
        drop(data);
        todo!();
    }

    /// update the free space map with the new free space on the page
    pub async fn update(&self, page: PageIndex, free_space: u16) -> nsql_buffer::Result<()> {
        assert!(free_space <= HeapTuple::MAX_SIZE);
        self.update_rec(self.meta.root_fsm_page, 0, page, free_space).await
    }

    async fn update_rec(
        &self,
        _fsm_page_idx: PageIndex,
        _level: u16,
        _page: PageIndex,
        _free_space: u16,
    ) -> nsql_buffer::Result<()> {
        let buffer = self.pool.load(self.meta.root_fsm_page).await?;
        let mut data = buffer.page().write();
        let _fsm_page = FsmPage::from_bytes_mut(&mut data);

        // let current_page = level * NODES_PER_PAGE;
        // let relative_offset = page - PageIndex::new(current_page);
        // let offset = fsm_page.update(relative_offset, free_space);

        drop(data);
        todo!()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fsm_page_tests;
