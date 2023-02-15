use std::fmt;

use crate::PAGE_SIZE;

#[derive(Clone)]
pub struct Page {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page").finish_non_exhaustive()
    }
}

impl Page {
    pub fn new(bytes: Box<[u8; PAGE_SIZE]>) -> Self {
        Self { bytes }
    }

    #[inline]
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageIndex(usize);

impl Default for PageIndex {
    fn default() -> Self {
        Self::INVALID
    }
}

impl PageIndex {
    pub(crate) const INVALID: Self = Self(usize::MAX);

    pub(crate) fn new(idx: usize) -> Self {
        Self(idx)
    }

    pub(crate) fn as_usize(self) -> usize {
        self.0
    }
}

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
