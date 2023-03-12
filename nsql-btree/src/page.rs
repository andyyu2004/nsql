use std::cmp::Ordering;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{AddAssign, Index, IndexMut, RangeFrom, Sub, SubAssign};
use std::pin::Pin;
use std::{cmp, fmt, io, mem, ptr, slice};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{Deserialize, DeserializeSkip, Serialize, SerializeSized};
use rkyv::rend::BigEndian;
use rkyv::Archive;

use crate::node::Flags;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";
const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

macro_rules! sizeof {
    ($ty:ty) => {
        mem::size_of::<$ty>() as u16
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive)]
#[archive_attr(derive(Debug))]
struct Slot {
    /// The offset of the entry from the start of the page
    offset: SlotOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, rkyv::Serialize)]
#[archive(as = "Self")]
#[repr(transparent)]
struct SlotOffset(BigEndian<u16>);

impl AddAssign<u16> for SlotOffset {
    fn add_assign(&mut self, rhs: u16) {
        self.0 += rhs;
    }
}

impl SubAssign<u16> for SlotOffset {
    fn sub_assign(&mut self, rhs: u16) {
        self.0 -= rhs;
    }
}

impl From<u16> for SlotOffset {
    fn from(offset: u16) -> Self {
        Self(BigEndian::from(offset))
    }
}

impl Sub for SlotOffset {
    type Output = u16;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

impl Sub<u16> for SlotOffset {
    type Output = SlotOffset;

    fn sub(self, rhs: u16) -> Self::Output {
        Self::from(self.0 - rhs)
    }
}

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug))]
pub(crate) struct LeafPageHeader {
    magic: [u8; 4],
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl ArchivedLeafPageHeader {
    fn check_magic(&self) -> nsql_serde::Result<()> {
        if self.magic != BTREE_LEAF_PAGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree leaf page header magic: {:#x?}", &self.magic[..]),
            ))?;
        }
        Ok(())
    }
}

impl Default for LeafPageHeader {
    fn default() -> Self {
        Self { magic: BTREE_LEAF_PAGE_MAGIC, prev: None, next: None }
    }
}

#[derive(Debug, SerializeSized, Deserialize)]
pub(crate) struct PageHeader {
    pub(crate) flags: Flags,
    padding: [u8; 3],
}

pub(crate) enum PageView<'a, K, V> {
    Leaf(LeafPageView<'a, K, V>),
}

impl<'a, K, V> PageView<'a, K, V>
where
    K: Ord + DeserializeSkip,
    V: Deserialize,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageView<'a, K, V>> {
        let header = PageHeader::deserialize(&mut &data[..]).await?;
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageView::create(&data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(Self::Leaf)
        } else {
            todo!()
        }
    }
}

pub(crate) struct LeafPageView<'a, K, V> {
    header: &'a ArchivedLeafPageHeader,
    slots: SlottedPageView<'a, K, V>,
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Ord + DeserializeSkip,
    V: Deserialize,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8],
    ) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_ref::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slots = SlottedPageView::<'a, K, V>::create(data).await?;
        Ok(Self { header, slots })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        self.slots.get(key).await
    }
}

pub(crate) enum PageViewMut<'a, K, V> {
    Leaf(LeafPageViewMut<'a, K, V>),
}

impl<'a, K, V> PageViewMut<'a, K, V> {
    pub(crate) async fn init_root(data: &'a mut [u8; PAGE_DATA_SIZE]) -> nsql_serde::Result<()> {
        Self::init_leaf_inner(data, Flags::IS_LEAF | Flags::IS_ROOT).await
    }

    pub(crate) async fn init_leaf(data: &'a mut [u8; PAGE_DATA_SIZE]) -> nsql_serde::Result<()> {
        Self::init_leaf_inner(data, Flags::IS_LEAF).await
    }

    async fn init_leaf_inner(
        data: &'a mut [u8; PAGE_DATA_SIZE],
        flags: Flags,
    ) -> nsql_serde::Result<()> {
        data.fill(0);
        PageHeader { flags, padding: [0; 3] }.serialize_into(data).await?;

        LeafPageViewMut::<K, V>::init(&mut data[PageHeader::SERIALIZED_SIZE as usize..]).await?;
        Ok(())
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageViewMut<'a, K, V>> {
        let header = PageHeader::deserialize(&mut &data[..]).await?;
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageViewMut::create(&mut data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(Self::Leaf)
        } else {
            todo!()
        }
    }

    pub(crate) fn unwrap_leaf(self) -> LeafPageViewMut<'a, K, V> {
        if let Self::Leaf(v) = self { v } else { panic!("node was not a leaf") }
    }
}

#[derive(Debug)]
pub(crate) struct LeafPageViewMut<'a, K, V> {
    header: Pin<&'a mut ArchivedLeafPageHeader>,
    slots: SlottedPageViewMut<'a, K, V>,
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    /// initialize a new leaf page
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<()> {
        const HEADER_SIZE: u16 = mem::size_of::<ArchivedLeafPageHeader>() as u16;
        let (header_bytes, data) = data.split_array_mut::<{ HEADER_SIZE as usize }>();
        header_bytes.copy_from_slice(&nsql_rkyv::archive(&LeafPageHeader::default()));
        // the slots start after the page header and the leaf page header
        let prefix_size = PageHeader::SERIALIZED_SIZE + HEADER_SIZE;
        SlottedPageViewMut::<'a, K, V>::init(data, prefix_size).await?;

        Ok(())
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_mut::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = rkyv::archived_root_mut::<LeafPageHeader>(Pin::new(header_bytes));
        header.check_magic()?;
        let slots = SlottedPageViewMut::create(data).await?;
        Ok(Self { header, slots })
    }

    fn downgrade(&'a self) -> LeafPageView<'a, K, V> {
        LeafPageView { header: &self.header, slots: self.slots.downgrade() }
    }
}

impl<'a, K, V> LeafPageViewMut<'a, K, V>
where
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
    V: Serialize + Deserialize + fmt::Debug,
{
    pub(crate) async fn insert(
        &mut self,
        key: K,
        value: V,
    ) -> nsql_serde::Result<Result<Option<V>, PageFull>> {
        self.slots.insert(key, value).await
    }

    /// Intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    /// We allocate two new nodes to move the data into.
    pub(crate) async fn split_root_into(
        &self,
        mut left: LeafPageViewMut<'a, K, V>,
        mut right: LeafPageViewMut<'a, K, V>,
    ) -> nsql_serde::Result<()> {
        assert!(left.slots.downgrade().is_empty());
        assert!(right.slots.downgrade().is_empty());

        // FIXME use less naive algorithm for inserting into new pages
        let (lhs, rhs) = self.slots.slots.split_at(self.slots.slots.len() / 2);
        for slot in lhs {
            let (key, value) = self.downgrade().slots.get_by_offset(slot.offset).await?;
            left.insert(key, value).await?.unwrap();
        }

        for slot in rhs {
            let (key, value) = self.downgrade().slots.get_by_offset(slot.offset).await?;
            right.insert(key, value).await?.unwrap();
        }
        Ok(())
    }
}

struct SlottedPageView<'a, K, V> {
    header: &'a ArchivedSlottedPageMeta,
    slots: &'a [Slot],
    data: &'a [u8],
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> fmt::Debug for SlottedPageView<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageView")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> SlottedPageView<'a, K, V>
where
    K: DeserializeSkip + Ord,
    V: Deserialize,
{
    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Safety: `buf` must contain a valid slotted page
    async unsafe fn create(buf: &'a [u8]) -> nsql_serde::Result<SlottedPageView<'a, K, V>> {
        let (header_bytes, buf) =
            buf.split_array_ref::<{ mem::size_of::<ArchivedSlottedPageMeta>() }>();
        let header = unsafe { rkyv::archived_root::<SlottedPageMeta>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at(slot_len * mem::size_of::<Slot>());

        let slots = unsafe { slice::from_raw_parts(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Ok(Self { header, slots, data, marker: PhantomData })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        let offset = match self.offset_of(key).await? {
            Some(offset) => offset,
            None => return Ok(None),
        };

        let (k, v) = self.get_by_offset(offset).await?;
        assert!(&k == key);
        Ok(Some(v))
    }

    async fn get_by_offset(&self, offset: SlotOffset) -> nsql_serde::Result<(K, V)> {
        let mut de = &self[offset..];
        let key = K::deserialize(&mut de).await?;
        let value = V::deserialize(&mut de).await?;
        Ok((key, value))
    }

    async fn slot_index_of(&self, key: &K) -> nsql_serde::Result<Result<usize, usize>> {
        async_binary_search_by(self.slots, |slot| async {
            let k = K::deserialize(&mut &self[slot.offset..]).await?;
            Ok(k.cmp(key))
        })
        .await
    }

    async fn slot_of(&self, key: &K) -> nsql_serde::Result<Option<&Slot>> {
        let offset = self.slot_index_of(key).await?;
        Ok(offset.ok().map(|offset| &self.slots[offset]))
    }

    async fn offset_of(&self, key: &K) -> nsql_serde::Result<Option<SlotOffset>> {
        self.slot_of(key).await.map(|slot| slot.map(|slot| slot.offset))
    }
}

impl<K, V> Index<RangeFrom<SlotOffset>> for SlottedPageView<'_, K, V> {
    type Output = [u8];

    fn index(&self, offset: RangeFrom<SlotOffset>) -> &Self::Output {
        let adjusted_offset = offset.start - self.header.slot_len * sizeof!(ArchivedSlot);
        &self.data[adjusted_offset.0.value() as usize..]
    }
}

impl<K, V> IndexMut<RangeFrom<SlotOffset>> for SlottedPageViewMut<'_, K, V> {
    // see SlottedPageViewMut::index
    fn index_mut(&mut self, offset: RangeFrom<SlotOffset>) -> &mut Self::Output {
        let adjusted_offset = offset.start - self.header.slot_len * sizeof!(ArchivedSlot);
        &mut self.data[adjusted_offset.0.value() as usize..]
    }
}

#[derive(Eq)]
#[repr(C)]
struct SlottedPageViewMut<'a, K, V> {
    header: Pin<&'a mut ArchivedSlottedPageMeta>,
    slots: &'a mut [Slot],
    data: &'a mut [u8],
    marker: PhantomData<(K, V)>,
}

impl<K, V> Index<RangeFrom<SlotOffset>> for SlottedPageViewMut<'_, K, V> {
    type Output = [u8];

    fn index(&self, offset: RangeFrom<SlotOffset>) -> &Self::Output {
        // adjust the offset to be relative to the start of the slots
        // as we keep shifting the slots and data around the actual offsets change dependending on the number of slots
        let adjusted_offset = offset.start - self.header.slot_len * sizeof!(ArchivedSlot);
        &self.data[adjusted_offset.0.value() as usize..]
    }
}

impl<'a, K, V> fmt::Debug for SlottedPageViewMut<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageViewMut")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> PartialEq for SlottedPageViewMut<'a, K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.slots == other.slots && self.data == other.data
    }
}

#[derive(Debug, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
#[archive(compare(PartialEq))]
struct SlottedPageMeta {
    // start offset initially starts from 0 and end offset is relative to that
    free_start: SlotOffset,
    free_end: SlotOffset,
    slot_len: u16,
}

nsql_util::static_assert_eq!(
    mem::size_of::<SlottedPageMeta>(),
    mem::size_of::<ArchivedSlottedPageMeta>()
);

impl<'a, K, V> SlottedPageViewMut<'a, K, V> {
    /// `prefix_size` is the size of the page header and the page-specific header` (and anything else that comes before the slots)
    /// slot offsets are relative to the initla value of `free_start`
    async fn init(buf: &'a mut [u8], prefix_size: u16) -> nsql_serde::Result<()> {
        let free_end = PAGE_DATA_SIZE as u16 - prefix_size - sizeof!(ArchivedSlottedPageMeta);
        assert_eq!(free_end, buf.len() as u16 - sizeof!(ArchivedSlottedPageMeta));
        let header = SlottedPageMeta {
            free_start: SlotOffset::from(0),
            free_end: SlotOffset::from(free_end),
            slot_len: 0,
        };
        let bytes = nsql_rkyv::archive(&header);
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(())
    }

    /// Safety: `buf` must point at the start of a valid slotted page
    async unsafe fn create(buf: &'a mut [u8]) -> nsql_serde::Result<SlottedPageViewMut<'a, K, V>> {
        let (header_bytes, buf) =
            buf.split_array_mut::<{ mem::size_of::<ArchivedSlottedPageMeta>() }>();
        let header = unsafe { rkyv::archived_root_mut::<SlottedPageMeta>(Pin::new(header_bytes)) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Slot>());

        let slots =
            unsafe { slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Ok(Self { header, slots, data, marker: PhantomData })
    }

    fn downgrade(&'a self) -> SlottedPageView<'a, K, V> {
        SlottedPageView {
            header: &self.header,
            slots: self.slots,
            data: self.data,
            marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PageFull;

impl<'a, K, V> SlottedPageViewMut<'a, K, V>
where
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
    V: Serialize + Deserialize + fmt::Debug,
{
    async fn insert(
        &mut self,
        key: K,
        value: V,
    ) -> nsql_serde::Result<Result<Option<V>, PageFull>> {
        // FIXME check whether key already exists
        // FIXME need to maintain sorted order in the slots
        let key_len = key.serialized_size().await?;
        let value_len = value.serialized_size().await?;
        let len = key_len + value_len;
        if len + sizeof!(ArchivedSlot) > self.header.free_end - self.header.free_start {
            return Ok(Err(PageFull));
        }

        let idx = self.downgrade().slot_index_of(&key).await?;

        let idx = match idx {
            Ok(idx) => todo!("handle case where key already exists {:?}", self.slots[idx]),
            Err(idx) => idx,
        };

        let key_offset = self.header.free_end - len;
        key.serialize_into(&mut self[key_offset..]).await?;

        let value_offset = self.header.free_end - value_len;
        value.serialize_into(&mut self[value_offset..]).await?;

        self.header.free_end -= len;
        self.header.slot_len += 1;

        unsafe {
            // write the new slot at index `idx` of the slot array and recreate the slice to include it

            // shift everything right of `idx` to the right by 1 (inclusive)
            ptr::copy(
                self.slots.as_ptr().add(idx),
                self.slots.as_mut_ptr().add(idx + 1),
                self.slots.len() - idx,
            );

            // write the new slot in the hole
            ptr::write(self.slots.as_mut_ptr().add(idx), Slot { offset: self.header.free_end });

            self.slots = slice::from_raw_parts_mut(
                self.slots.as_mut_ptr(),
                self.header.slot_len.value() as usize,
            );
        }

        self.header.free_start += sizeof!(ArchivedSlot);

        // we have to shift over the slice of data as we expect it to start after the slots (at `free_start`)
        // we some small tricks to avoid lifetime issues without using unsafe
        // see https://stackoverflow.com/questions/61223234/can-i-reassign-a-mutable-slice-reference-to-a-sub-slice-of-itself
        let data: &'a mut [u8] = mem::take(&mut self.data);
        let (_, data) = data.split_array_mut::<{ mem::size_of::<ArchivedSlot>() }>();
        self.data = data;

        #[cfg(debug_assertions)]
        self.assert_sorted().await?;

        Ok(Ok(None))
    }

    #[cfg(debug_assertions)]
    async fn assert_sorted(&self) -> nsql_serde::Result<()> {
        let mut keys = Vec::with_capacity(self.header.slot_len.value() as usize);
        for slot in self.slots.iter() {
            let k = K::deserialize(&mut &self[slot.offset..]).await?;
            keys.push(k);
        }

        assert!(keys.is_sorted(), "{:?}", keys);
        Ok(())
    }
}

// adapted from std
async fn async_binary_search_by<'a, T, F, Fut>(
    xs: &'a [T],
    f: F,
) -> nsql_serde::Result<Result<usize, usize>>
where
    T: Copy,
    F: Fn(&'a T) -> Fut + 'a,
    Fut: Future<Output = nsql_serde::Result<Ordering>> + 'a,
{
    let mut size = xs.len();
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;

        let cmp = f(unsafe { xs.get_unchecked(mid) }).await?;

        if cmp == Ordering::Less {
            left = mid + 1;
        } else if cmp == Ordering::Greater {
            right = mid;
        } else {
            return Ok(Ok(mid));
        }

        size = right - left;
    }

    Ok(Err(left))
}
