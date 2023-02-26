use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use nsql_serde::{AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, Serializer};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{CHECKSUM_LENGTH, PAGE_SIZE, RAW_PAGE_SIZE};

#[derive(Clone)]
pub struct Page {
    idx: PageIndex,
    bytes: Arc<RwLock<[u8; RAW_PAGE_SIZE]>>,
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page").finish_non_exhaustive()
    }
}

impl Page {
    #[inline]
    pub fn idx(&self) -> PageIndex {
        self.idx
    }

    /// Get an immutable reference to the data bytes of the page
    #[inline]
    pub fn data(&self) -> ReadonlyPageView<'_> {
        let bytes = self.bytes.read();
        ReadonlyPageView { bytes }
    }

    #[inline]
    pub fn data_mut(&self) -> WriteablePageView<'_> {
        let bytes = self.bytes.write();
        WriteablePageView { bytes }
    }

    #[inline]
    pub(crate) fn new(idx: PageIndex, bytes: [u8; RAW_PAGE_SIZE]) -> Self {
        Self { idx, bytes: Arc::new(RwLock::new(bytes)) }
    }

    #[inline]
    pub(crate) fn zeroed(idx: PageIndex) -> Self {
        Self::new(idx, [0; RAW_PAGE_SIZE])
    }

    #[inline]
    pub(crate) fn bytes(&self) -> RwLockReadGuard<'_, [u8; RAW_PAGE_SIZE]> {
        self.bytes.read()
    }

    /// Read the checksum from the page header
    #[inline]
    pub(crate) fn expected_checksum(&self) -> u64 {
        u64::from_be_bytes(self.bytes()[..CHECKSUM_LENGTH].try_into().unwrap())
    }

    #[inline]
    pub(crate) fn update_checksum(&mut self) {
        let checksum = self.compute_checksum();
        self.bytes.write()[0..8].copy_from_slice(&checksum.to_be_bytes());
        assert!(self.expected_checksum() == checksum);
    }

    /// Compute the checksum of the page and write it to the first 8 bytes of the page.
    #[inline]
    pub(crate) fn compute_checksum(&self) -> u64 {
        checksum(self.data().as_ref())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct PageIndex(u32);

impl Serialize for PageIndex {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_u32(self.0).await
    }
}

impl Deserialize for PageIndex {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        Ok(Self(de.read_u32().await?))
    }
}

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
    pub const fn new(idx: u32) -> Self {
        assert!(idx < u32::MAX, "page index is too large");
        Self(idx)
    }

    #[inline]
    pub const fn new_maybe_invalid(idx: u32) -> Self {
        Self(idx)
    }

    #[inline]
    pub(crate) fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub(crate) fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[inline]
    pub(super) fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct ReadonlyPageView<'a> {
    bytes: RwLockReadGuard<'a, [u8; RAW_PAGE_SIZE]>,
}

impl Deref for ReadonlyPageView<'_> {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.bytes[CHECKSUM_LENGTH..].as_ptr() as *const [u8; PAGE_SIZE]) }
    }
}

#[cfg(test)]
impl<R> PartialEq<R> for ReadonlyPageView<'_>
where
    R: AsRef<[u8; PAGE_SIZE]>,
{
    fn eq(&self, other: &R) -> bool {
        self.as_ref() == other.as_ref()
    }
}

#[derive(Debug)]
pub struct WriteablePageView<'a> {
    bytes: RwLockWriteGuard<'a, [u8; RAW_PAGE_SIZE]>,
}

impl Deref for WriteablePageView<'_> {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.bytes[CHECKSUM_LENGTH..].as_ptr() as *const [u8; PAGE_SIZE]) }
    }
}

impl DerefMut for WriteablePageView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.bytes[CHECKSUM_LENGTH..].as_mut_ptr() as *mut [u8; PAGE_SIZE]) }
    }
}

fn checksum(data: impl AsRef<[u8]>) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_WE).checksum(data.as_ref())
}
