use std::num::NonZeroU32;
use std::ops::{Add, Deref, DerefMut, Sub};
use std::sync::Arc;
use std::{fmt, mem};

use nsql_serde::{SerializeSized, StreamDeserialize, StreamSerialize};
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{PAGE_DATA_SIZE, PAGE_META_LENGTH, PAGE_SIZE};

/// A page of data in the database allocated by the pager.
/// The data is aligned to 16 bytes.
#[derive(Clone)]
pub struct Page {
    idx: PageIndex,
    bytes: Arc<RwLock<rkyv::AlignedBytes<PAGE_SIZE>>>,
}

// this is used purely to make `Page` `mem::take`able
impl Default for Page {
    fn default() -> Self {
        Self::zeroed(PageIndex::INVALID)
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page").finish_non_exhaustive()
    }
}

impl Page {
    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.idx
    }

    #[inline]
    pub fn arced_data(&self) -> Arc<RwLock<rkyv::AlignedBytes<PAGE_SIZE>>> {
        Arc::clone(&self.bytes)
    }

    /// Lock the page with a read lock an immutable reference to the data bytes of the page
    #[inline]
    pub async fn read(&self) -> PageReadGuard<'_> {
        let idx = self.page_idx();
        tracing::trace!(?idx, "read-lock page");
        let bytes = self.bytes.read().await;
        PageReadGuard { idx, bytes }
    }

    #[inline]
    pub fn blocking_read(&self) -> PageReadGuard<'_> {
        let idx = self.page_idx();
        tracing::trace!(?idx, "blocking read-lock page");
        let bytes = self.bytes.blocking_read();
        PageReadGuard { idx, bytes }
    }

    /// Lock the page with a write lock and a mutable reference to the data bytes of the page
    #[inline]
    pub async fn write(&self) -> PageWriteGuard<'_> {
        let idx = self.page_idx();
        let bytes = self.bytes.write().await;
        tracing::trace!(?idx, "write-lock page");
        PageWriteGuard { page_idx: idx, bytes }
    }

    #[inline]
    pub fn blocking_write(&self) -> PageWriteGuard<'_> {
        let idx = self.page_idx();
        let bytes = self.bytes.blocking_write();
        tracing::trace!(?idx, "blocking write-lock page");
        PageWriteGuard { page_idx: idx, bytes }
    }

    #[inline]
    pub(crate) fn new(idx: PageIndex, bytes: rkyv::AlignedBytes<PAGE_SIZE>) -> Self {
        Self { idx, bytes: Arc::new(RwLock::new(bytes)) }
    }

    #[inline]
    pub(crate) fn zeroed(idx: PageIndex) -> Self {
        Self::new(idx, rkyv::AlignedBytes([0; PAGE_SIZE]))
    }

    #[inline]
    pub(crate) async fn bytes(&self) -> RwLockReadGuard<'_, rkyv::AlignedBytes<PAGE_SIZE>> {
        self.bytes.read().await
    }

    /// Read the checksum from the page header
    #[inline]
    pub(crate) async fn expected_checksum(&self) -> u64 {
        u64::from_be_bytes(self.bytes().await[..PAGE_META_LENGTH].try_into().unwrap())
    }

    #[inline]
    pub(crate) async fn update_checksum(&mut self) {
        let checksum = self.compute_checksum().await;
        self.bytes.write().await[0..8].copy_from_slice(&checksum.to_be_bytes());
        assert!(self.expected_checksum().await == checksum);
    }

    /// Compute the checksum of the page and write it to the first 8 bytes of the page.
    #[inline]
    pub(crate) async fn compute_checksum(&self) -> u64 {
        checksum(self.read().await.as_ref())
    }
}

// Internally one indexed to enable niche optimization.
// However, we do the adjustments on construction and retrieval to make it transparent to the user
#[derive(
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    StreamDeserialize,
    SerializeSized,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[repr(transparent)]
#[archive(compare(PartialEq, PartialOrd))]
#[archive_attr(derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord))]
pub struct PageIndex {
    idx: NonZeroU32,
}

impl fmt::Debug for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u32())
    }
}

impl From<Archived<PageIndex>> for PageIndex {
    fn from(value: Archived<PageIndex>) -> Self {
        Self { idx: value.idx.value() }
    }
}

impl From<PageIndex> for ArchivedPageIndex {
    fn from(idx: PageIndex) -> Self {
        Self { idx: idx.idx.into() }
    }
}

static_assert_eq!(mem::size_of::<PageIndex>(), 4);

impl PageIndex {
    // do not make this public, it is mostly a hack that we haven't entirely managed to get rid of
    // it is used in the meta page reader
    pub(crate) const INVALID: Self = Self { idx: unsafe { NonZeroU32::new_unchecked(u32::MAX) } };
    pub const ZERO: Self = Self::new(0);

    #[inline]
    pub(crate) const fn new(idx: u32) -> Self {
        assert!(idx < u32::MAX, "u32::MAX is reserved for invalid page index");
        Self { idx: unsafe { NonZeroU32::new_unchecked(idx + 1) } }
    }

    #[inline]
    pub(crate) fn new_maybe_invalid(idx: u32) -> Option<Self> {
        if idx == Self::INVALID.as_u32() { None } else { Some(Self::new(idx)) }
    }

    #[inline]
    pub(crate) fn is_zero(self) -> bool {
        self.as_u32() == 0
    }

    #[inline]
    pub(super) fn as_u32(self) -> u32 {
        // adjust for the fact that we are one indexed
        self.idx.get() - 1
    }
}

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.idx)
    }
}

#[derive(Debug)]
pub struct PageReadGuard<'a> {
    idx: PageIndex,
    bytes: RwLockReadGuard<'a, rkyv::AlignedBytes<PAGE_SIZE>>,
}

impl PageReadGuard<'_> {
    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.idx
    }
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = [u8; PAGE_DATA_SIZE];

    fn deref(&self) -> &'a Self::Target {
        unsafe { &*(self.bytes[PAGE_META_LENGTH..].as_ptr() as *const [u8; PAGE_DATA_SIZE]) }
    }
}

#[cfg(test)]
impl<R> PartialEq<R> for PageReadGuard<'_>
where
    R: AsRef<[u8; PAGE_DATA_SIZE]>,
{
    fn eq(&self, other: &R) -> bool {
        self.as_ref() == other.as_ref()
    }
}

pub struct PageWriteGuard<'a> {
    page_idx: PageIndex,
    bytes: RwLockWriteGuard<'a, rkyv::AlignedBytes<PAGE_SIZE>>,
}

impl fmt::Debug for PageWriteGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PageWriteGuard").field("page_idx", &self.page_idx).finish_non_exhaustive()
    }
}

impl<'a> PageWriteGuard<'a> {
    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.page_idx
    }
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = [u8; PAGE_DATA_SIZE];

    fn deref(&self) -> &'a Self::Target {
        unsafe { &*(self.bytes[PAGE_META_LENGTH..].as_ptr() as *const [u8; PAGE_DATA_SIZE]) }
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &'a mut Self::Target {
        unsafe { &mut *(self.bytes[PAGE_META_LENGTH..].as_mut_ptr() as *mut [u8; PAGE_DATA_SIZE]) }
    }
}

fn checksum(data: impl AsRef<[u8]>) -> u64 {
    crc::Crc::<u64>::new(&crc::CRC_64_WE).checksum(data.as_ref())
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, StreamSerialize, StreamDeserialize,
)]
pub struct PageOffset {
    offset: u32,
}

impl PageOffset {
    #[inline]
    pub fn new(offset: u32) -> PageOffset {
        Self { offset }
    }

    #[inline]
    pub fn as_u32(self) -> u32 {
        self.offset
    }
}

impl Add<PageOffset> for PageIndex {
    type Output = PageIndex;

    fn add(self, rhs: PageOffset) -> Self::Output {
        PageIndex::new(self.idx.get() + rhs.offset)
    }
}

impl Add for PageOffset {
    type Output = PageOffset;

    fn add(self, rhs: Self) -> Self::Output {
        PageOffset::new(self.offset + rhs.offset)
    }
}

impl Sub<PageOffset> for PageIndex {
    type Output = PageIndex;

    fn sub(self, rhs: PageOffset) -> Self::Output {
        PageIndex::new(self.idx.get() - rhs.offset)
    }
}

#[cfg(test)]
impl proptest::arbitrary::Arbitrary for PageIndex {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::Strategy;
        (0..1000u32).prop_map(PageIndex::new).boxed()
    }
}
