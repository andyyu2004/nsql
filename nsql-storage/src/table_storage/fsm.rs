mod fsm_page;

use nsql_pager::PageIndex;

use self::fsm_page::FsmPage;
use crate::table_storage::HeapTuple;

pub struct FreeSpaceMap {}

impl FreeSpaceMap {
    /// find a page with at least `required_size` free space, returning the page index
    pub fn find(&self, required_size: u16) -> Option<PageIndex> {
        assert!(required_size > 0);
        assert!(required_size <= HeapTuple::MAX_SIZE);
        todo!()
    }

    /// update the free space map with the new free space on the page
    pub fn update(&mut self, page: PageIndex, free_space: u16) {
        todo!();
    }
}

#[cfg(test)]
mod tests;
