use std::marker::PhantomData;
use std::ops::{AddAssign, Index, IndexMut, RangeFrom, Sub, SubAssign};
use std::pin::Pin;
use std::{fmt, io, mem, ptr, slice};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{Deserialize, Serialize, SerializeSized};
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
    K: Ord + Deserialize,
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
    K: Ord + Deserialize,
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
        // dbg!(&self.header);
        // dbg!(&self.slots);
        self.slots.get(key).await
    }
}

pub(crate) enum PageViewMut<'a, K, V> {
    Leaf(LeafPageViewMut<'a, K, V>),
}

impl<'a, K, V> PageViewMut<'a, K, V> {
    pub(crate) async fn init_root(data: &'a mut [u8; PAGE_DATA_SIZE]) -> nsql_serde::Result<()> {
        data.fill(0);
        PageHeader { flags: Flags::IS_LEAF | Flags::IS_ROOT, padding: [0; 3] }
            .serialize_into(data)
            .await?;

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
}

impl<'a, K, V> LeafPageViewMut<'a, K, V>
where
    K: Serialize + Deserialize + Ord + fmt::Debug,
    V: Serialize + Deserialize + Eq + fmt::Debug,
{
    pub(crate) async fn insert(&mut self, key: K, value: V) -> nsql_serde::Result<Option<V>> {
        self.slots.insert(key, value).await?;
        Ok(None)
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
    K: Deserialize + Ord,
    V: Deserialize,
{
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
        for slot in self.slots.iter() {
            let mut de = &self[slot.offset..];
            let k = K::deserialize(&mut de).await?;
            if &k == key {
                return V::deserialize(&mut de).await.map(Some);
            }
        }

        Ok(None)
    }
}

impl<K, V> Index<RangeFrom<SlotOffset>> for SlottedPageView<'_, K, V> {
    type Output = [u8];

    // see SlottedPageViewMut::index
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
}

impl<'a, K, V> SlottedPageViewMut<'a, K, V>
where
    K: Serialize + Deserialize + Ord + fmt::Debug,
    V: Serialize + Deserialize + Eq + fmt::Debug,
{
    async fn insert(&mut self, key: K, value: V) -> nsql_serde::Result<()> {
        // FIXME check there is sufficient space
        // TODO check whether key already exists
        let key_len = key.serialized_size().await?;
        let value_len = value.serialized_size().await?;
        let len = key_len + value_len;
        if len + sizeof!(ArchivedSlot) > self.header.free_end - self.header.free_start {
            todo!()
        }

        let key_offset = self.header.free_end - len;
        key.serialize_into(&mut self[key_offset..]).await?;

        let value_offset = self.header.free_end - value_len;
        value.serialize_into(&mut self[value_offset..]).await?;

        self.header.free_end -= len;
        self.header.slot_len += 1;

        unsafe {
            // write the new slot at the conceptual end of the slot array and recreate the slice to include it
            ptr::write(
                self.slots.as_mut_ptr().add(self.slots.len()),
                Slot { offset: self.header.free_end },
            );
            self.slots = slice::from_raw_parts_mut(
                self.slots.as_mut_ptr(),
                self.header.slot_len.value() as usize,
            );
        }

        self.header.free_start += sizeof!(ArchivedSlot);

        Ok(())
    }
}
