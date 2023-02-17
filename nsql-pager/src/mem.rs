use parking_lot::RwLock;

use crate::{Page, PageIndex, Pager, Result};

#[derive(Default, Debug)]
pub struct InMemoryPager {
    pages: RwLock<Vec<Page>>,
    free_pages: RwLock<Vec<PageIndex>>,
}

impl InMemoryPager {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Pager for InMemoryPager {
    #[inline]
    async fn alloc_page(&self) -> Result<PageIndex> {
        let mut pages = self.pages.write();
        let idx =
            self.free_pages.write().pop().unwrap_or_else(|| PageIndex::new(pages.len() as u32));
        pages.push(Page::zeroed());
        Ok(idx)
    }

    #[inline]
    async fn free_page(&self, idx: PageIndex) -> Result<()> {
        self.free_pages.write().push(idx);
        Ok(())
    }

    #[inline]
    async fn read_page(&self, idx: PageIndex) -> Result<Page> {
        Ok(self.pages.read()[idx.as_u32() as usize].clone())
    }

    #[inline]
    async fn write_page(&self, idx: PageIndex, page: Page) -> Result<()> {
        self.pages.write()[idx.as_u32() as usize] = page;
        Ok(())
    }
}
