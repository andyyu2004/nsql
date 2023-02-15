use parking_lot::RwLock;

use crate::{Page, PageIndex, Pager, Result, PAGE_SIZE};

#[derive(Default)]
pub struct InMemoryPager {
    pages: RwLock<Vec<Page>>,
}

impl Pager for InMemoryPager {
    async fn alloc_page(&self) -> Result<PageIndex> {
        let mut pages = self.pages.write();
        let idx = pages.len();
        pages.push(Page::new(Box::new([0; PAGE_SIZE])));
        Ok(PageIndex::new(idx))
    }

    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        Ok(self.pages.read()[idx.as_usize()].clone())
    }

    async fn write_page(&self, idx: PageIndex, page: Page) -> Result<()> {
        self.pages.write()[idx.as_usize()] = page;
        Ok(())
    }
}
