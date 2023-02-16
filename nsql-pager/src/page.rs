use std::fmt;

use crate::{CHECKSUM_LENGTH, PAGE_SIZE, RAW_PAGE_SIZE};

#[derive(Clone)]
pub struct Page {
    bytes: Box<[u8; RAW_PAGE_SIZE]>,
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page").finish_non_exhaustive()
    }
}

impl Page {
    /// Get an immutable reference to the data bytes of the page
    #[inline]
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        (&self.bytes[CHECKSUM_LENGTH..]).try_into().unwrap()
    }

    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        unsafe { &mut *(self.bytes[CHECKSUM_LENGTH..].as_mut_ptr() as *mut [u8; PAGE_SIZE]) }
    }

    #[inline]
    pub(crate) fn new(bytes: [u8; RAW_PAGE_SIZE]) -> Self {
        Self { bytes: Box::new(bytes) }
    }

    #[inline]
    pub(crate) fn zeroed() -> Self {
        Self::new([0; RAW_PAGE_SIZE])
    }

    #[inline]
    pub(crate) fn bytes(&self) -> &[u8; RAW_PAGE_SIZE] {
        &self.bytes
    }

    /// Read the checksum from the page header
    #[inline]
    pub(crate) fn expected_checksum(&self) -> u64 {
        u64::from_be_bytes(self.bytes()[..CHECKSUM_LENGTH].try_into().unwrap())
    }

    #[inline]
    pub(crate) fn update_checksum(&mut self) {
        let checksum = self.compute_checksum();
        self.bytes[0..8].copy_from_slice(&checksum.to_be_bytes());
        assert!(self.expected_checksum() == checksum);
    }

    /// Compute the checksum of the page and write it to the first 8 bytes of the page.
    #[inline]
    pub(crate) fn compute_checksum(&self) -> u64 {
        checksum(self.data())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageIndex(u32);

#[cfg(test)]
impl proptest::arbitrary::Arbitrary for PageIndex {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::Strategy;
        (0..1000u32).prop_map(PageIndex).boxed()
    }
}

impl Default for PageIndex {
    fn default() -> Self {
        Self::INVALID
    }
}

impl PageIndex {
    pub(crate) const INVALID: Self = Self(u32::MAX);

    #[inline]
    pub(crate) const fn new(idx: u32) -> Self {
        assert!(idx < u32::MAX, "page index is too large");
        Self(idx)
    }

    #[inline]
    pub(crate) fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn checksum(data: &[u8]) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_WE).checksum(data)
}
