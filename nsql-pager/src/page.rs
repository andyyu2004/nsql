use std::io::Write;
use std::num::NonZeroU32;
use std::ops::{Add, Deref, DerefMut, Sub};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io, mem};

use nsql_serde::{Deserialize, Serialize, SerializeSized};
use nsql_util::static_assert_eq;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rkyv::{Archive, Archived};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{PAGE_DATA_SIZE, PAGE_META_LENGTH, PAGE_SIZE};

#[derive(Clone)]
pub struct Page {
    idx: PageIndex,
    bytes: Arc<RwLock<[u8; PAGE_SIZE]>>,
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
    pub fn arced_data(&self) -> Arc<RwLock<[u8; PAGE_SIZE]>> {
        Arc::clone(&self.bytes)
    }

    /// Lock the page with a read lock an immutable reference to the data bytes of the page
    #[inline]
    pub fn read(&self) -> PageReadGuard<'_> {
        let idx = self.page_idx();
        tracing::trace!(?idx, "read-lock page");
        let bytes = self.bytes.read();
        PageReadGuard { idx, bytes, read_offset: PAGE_META_LENGTH }
    }

    /// Lock the page with a write lock and a mutable reference to the data bytes of the page
    #[inline]
    pub fn write(&self) -> PageWriteGuard<'_> {
        let idx = self.page_idx();
        let bytes = self.bytes.write();
        tracing::trace!(?idx, "write-lock page");
        PageWriteGuard { idx, bytes, write_offset: PAGE_META_LENGTH }
    }

    #[inline]
    pub(crate) fn new(idx: PageIndex, bytes: [u8; PAGE_SIZE]) -> Self {
        Self { idx, bytes: Arc::new(RwLock::new(bytes)) }
    }

    #[inline]
    pub(crate) fn zeroed(idx: PageIndex) -> Self {
        Self::new(idx, [0; PAGE_SIZE])
    }

    #[inline]
    pub(crate) fn bytes(&self) -> RwLockReadGuard<'_, [u8; PAGE_SIZE]> {
        self.bytes.read()
    }

    /// Read the checksum from the page header
    #[inline]
    pub(crate) async fn expected_checksum(&self) -> u64 {
        u64::from_be_bytes(self.bytes()[..PAGE_META_LENGTH].try_into().unwrap())
    }

    #[inline]
    pub(crate) async fn update_checksum(&mut self) {
        let checksum = self.compute_checksum().await;
        self.bytes.write()[0..8].copy_from_slice(&checksum.to_be_bytes());
        assert!(self.expected_checksum().await == checksum);
    }

    /// Compute the checksum of the page and write it to the first 8 bytes of the page.
    #[inline]
    pub(crate) async fn compute_checksum(&self) -> u64 {
        checksum(self.read().as_ref())
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
    Deserialize,
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
    bytes: RwLockReadGuard<'a, [u8; PAGE_SIZE]>,
    read_offset: usize,
}

impl PageReadGuard<'_> {
    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.idx
    }
}

impl AsyncRead for PageReadGuard<'_> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let offset = self.read_offset;
        if offset >= PAGE_SIZE {
            return Poll::Ready(Ok(()));
        }
        let amt = buf.remaining().min(self.bytes.len() - offset);
        buf.put_slice(&self.bytes[offset..offset + amt]);
        self.read_offset += amt;
        Poll::Ready(Ok(()))
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

#[derive(Debug)]
pub struct PageWriteGuard<'a> {
    idx: PageIndex,
    bytes: RwLockWriteGuard<'a, [u8; PAGE_SIZE]>,
    write_offset: usize,
}

impl<'a> PageWriteGuard<'a> {
    #[inline]
    pub fn page_idx(&self) -> PageIndex {
        self.idx
    }
}

impl AsyncWrite for PageWriteGuard<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let offset = self.write_offset;
        if offset + buf.len() > PAGE_SIZE {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "write to page exceeds page size ({} > {PAGE_DATA_SIZE})",
                    offset + buf.len()
                ),
            )));
        }

        let n = self.bytes[offset..].as_mut().write(buf)?;
        self.write_offset += n;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
