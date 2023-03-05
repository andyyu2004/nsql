use std::io::{Read, Write};
use std::ops::{Add, Deref, DerefMut, Sub};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

use nsql_serde::{Deserialize, Invalid, Serialize, Serializer};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{CHECKSUM_LENGTH, PAGE_DATA_SIZE, PAGE_SIZE};

#[derive(Clone)]
pub struct Page {
    idx: PageIndex,
    bytes: Arc<RwLock<[u8; PAGE_SIZE]>>,
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

    #[inline]
    pub fn arced_data(&self) -> Arc<RwLock<[u8; PAGE_SIZE]>> {
        Arc::clone(&self.bytes)
    }

    /// Get an immutable reference to the data bytes of the page
    #[inline]
    pub fn data(&self) -> ReadonlyPageView<'_> {
        let bytes = self.bytes.read();
        ReadonlyPageView { bytes, read_offset: 0 }
    }

    #[inline]
    pub fn data_mut(&self) -> WriteablePageView<'_> {
        let bytes = self.bytes.write();
        WriteablePageView { bytes, write_offset: 0 }
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct PageIndex {
    idx: u32,
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

impl Invalid for PageIndex {
    #[inline]
    fn invalid() -> Self {
        Self::INVALID
    }
}

impl PageIndex {
    pub(crate) const INVALID: Self = Self { idx: u32::MAX };

    #[inline]
    pub(crate) const fn new(idx: u32) -> Self {
        assert!(idx < u32::MAX, "page index is too large");
        Self { idx }
    }

    #[inline]
    pub const fn new_maybe_invalid(idx: u32) -> Self {
        Self { idx }
    }

    #[inline]
    pub(crate) fn is_zero(self) -> bool {
        self.idx == 0
    }

    #[inline]
    pub(crate) fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    #[inline]
    pub(super) fn as_u32(self) -> u32 {
        self.idx
    }
}

impl fmt::Display for PageIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.idx)
    }
}

#[derive(Debug)]
pub struct ReadonlyPageView<'a> {
    bytes: RwLockReadGuard<'a, [u8; PAGE_SIZE]>,
    read_offset: usize,
}

impl AsyncRead for ReadonlyPageView<'_> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let offset = self.read_offset;
        let amt = buf.remaining().min(self.bytes.len() - offset);
        buf.put_slice(&self.bytes[offset..offset + amt]);
        self.read_offset += amt;
        Poll::Ready(Ok(()))
    }
}

impl Deref for ReadonlyPageView<'_> {
    type Target = [u8; PAGE_DATA_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.bytes[CHECKSUM_LENGTH..].as_ptr() as *const [u8; PAGE_DATA_SIZE]) }
    }
}

#[cfg(test)]
impl<R> PartialEq<R> for ReadonlyPageView<'_>
where
    R: AsRef<[u8; PAGE_DATA_SIZE]>,
{
    fn eq(&self, other: &R) -> bool {
        self.as_ref() == other.as_ref()
    }
}

#[derive(Debug)]
pub struct WriteablePageView<'a> {
    bytes: RwLockWriteGuard<'a, [u8; PAGE_SIZE]>,
    write_offset: usize,
}

impl AsyncWrite for WriteablePageView<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.write_offset + buf.len() > PAGE_DATA_SIZE {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "write to page exceeds page size",
            )));
        }

        let offset = self.write_offset;
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

impl Deref for WriteablePageView<'_> {
    type Target = [u8; PAGE_DATA_SIZE];

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.bytes[CHECKSUM_LENGTH..].as_ptr() as *const [u8; PAGE_DATA_SIZE]) }
    }
}

impl DerefMut for WriteablePageView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.bytes[CHECKSUM_LENGTH..].as_mut_ptr() as *mut [u8; PAGE_DATA_SIZE]) }
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
        PageIndex::new(self.idx + rhs.offset)
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
        PageIndex::new(self.idx - rhs.offset)
    }
}
