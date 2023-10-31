use std::ops::Mul;

use crate::{Env, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct PageHeader {
    magic: [u8; 4],
    kind: PageKind,
}

impl PageHeader {
    const MAGIC: [u8; 4] = *b"PAGE";
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub(crate) enum PageKind {
    Leaf,
    Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct PageIdx(usize);

impl PageIdx {
    pub const INVALID: Self = Self(usize::MAX);
}

impl Mul<usize> for PageIdx {
    type Output = usize;

    fn mul(self, rhs: usize) -> Self::Output {
        self.0 * rhs
    }
}

impl Env {
    pub(crate) fn search(&self, key: &[u8]) -> Option<PageIdx> {
        if self.root_page == PageIdx::INVALID {
            return None;
        }

        Some(self.search_page(self.root_page, key))
    }

    fn search_page(&self, page: PageIdx, key: &[u8]) -> PageIdx {
        let page = self.read_page(page);
        todo!();
    }

    fn read_page(&self, page: PageIdx) -> &PageHeader {
        let start = page * self.page_size;
        let page = unsafe {
            &*(&self.mmap[start..start + self.page_size] as *const _ as *const PageHeader)
        };
        assert_eq!(page.magic, PageHeader::MAGIC);
        page
    }

    fn new_page(&mut self, kind: PageKind) -> Result<PageIdx> {
        let page = self.mmap.len() / self.page_size;
        // FIXME obviously wildly inefficient to grow like this
        unsafe { self.mmap.remap(self.mmap.len() + self.page_size, memmap2::RemapOptions::new())? };
        let page_idx = PageIdx(page / self.page_size);
        let page = self.read_page(page_idx);
        // page.magic = PageHeader::MAGIC;
        // page.kind = kind;
        todo!();
        // Ok(page)
    }
}
