use std::marker::PhantomData;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[archive_attr(derive(Debug))]
struct Slot {
    /// The offset of the entry from the start of the page
    offset: BigEndian<u16>,
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
    slots: SlottedPageView<'a, (K, V)>,
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

        if header.magic != BTREE_LEAF_PAGE_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree leaf header magic: {:?}", header.magic),
            ))?;
        }

        let slots = SlottedPageView::<'a, (K, V)>::create(data).await?;
        Ok(Self { header, slots })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        dbg!(&self.header);
        dbg!(&self.slots);
        todo!();
        // for slot in self.slots.iter() {
        //     let offset = slot.offset.value() as usize;
        //     let mut de = &self.data[offset..];
        //     let k = K::deserialize(&mut de).await?;
        //     // FIXME better format so we don't have to deserialize values too
        //     let v = V::deserialize(&mut de).await.ok();
        //     if k == *key {
        //         return Ok(v);
        //     }
        // }
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
        let mut de = &data[..];
        let header = PageHeader::deserialize(&mut de).await?;
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
    slots: SlottedPageViewMut<'a, (K, V)>,
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    /// initialize a new leaf page
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<()> {
        let header_bytes = nsql_rkyv::archive(&LeafPageHeader::default());
        data[0..header_bytes.len()].copy_from_slice(&header_bytes);
        // the slots start after the page header and the leaf page header
        const HEADER_SIZE: u16 = mem::size_of::<ArchivedLeafPageHeader>() as u16;
        let prefix_size = PageHeader::SERIALIZED_SIZE + HEADER_SIZE;
        SlottedPageViewMut::<(K, V)>::init(&mut data[HEADER_SIZE as usize..], prefix_size).await?;

        Ok(())
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_mut::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = nsql_rkyv::unarchive_root_mut::<LeafPageHeader>(Pin::new(header_bytes));
        header.check_magic()?;
        let view = SlottedPageViewMut::create(data).await?;
        Ok(Self { header, slots: view })
    }
}

impl<'a, K: Serialize, V: Serialize> LeafPageViewMut<'a, K, V> {
    pub(crate) async fn insert(&mut self, key: K, value: V) -> nsql_serde::Result<Option<V>> {
        self.slots.insert((key, value)).await?;
        Ok(None)
    }
}

struct SlottedPageView<'a, T> {
    header: &'a ArchivedSlottedPageMeta,
    slots: &'a [Slot],
    data: &'a [u8],
    marker: PhantomData<T>,
}

impl<'a, T> fmt::Debug for SlottedPageView<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageView")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .finish_non_exhaustive()
    }
}

impl<'a, T: Deserialize> SlottedPageView<'a, T> {
    /// Safety: `data` must be a valid slotted page with the correct header type
    async unsafe fn create(buf: &'a [u8]) -> nsql_serde::Result<SlottedPageView<'a, T>> {
        let (header_bytes, buf) =
            buf.split_array_ref::<{ mem::size_of::<ArchivedSlottedPageMeta>() }>();
        let header = unsafe { rkyv::archived_root::<SlottedPageMeta>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at(slot_len * mem::size_of::<Slot>());

        let slots = unsafe { slice::from_raw_parts(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Ok(Self { header, slots, data, marker: PhantomData })
    }

    pub(crate) async fn get(&mut self, idx: u16) -> nsql_serde::Result<T> {
        let slot = self.slots[idx as usize];
        let offset = slot.offset.value() as usize;
        T::deserialize(&mut &self.data[offset..]).await
    }
}

#[derive(Eq)]
#[repr(C)]
struct SlottedPageViewMut<'a, T> {
    header: Pin<&'a mut ArchivedSlottedPageMeta>,
    slots: &'a mut [Slot],
    data: &'a mut [u8],
    marker: PhantomData<T>,
}

impl<'a, T> fmt::Debug for SlottedPageViewMut<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageViewMut")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl<'a, T> PartialEq for SlottedPageViewMut<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.slots == other.slots && self.data == other.data
    }
}

#[derive(Debug, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
#[archive(compare(PartialEq))]
struct SlottedPageMeta {
    free_start: u16,
    free_end: u16,
    slot_len: u16,
}

nsql_util::static_assert_eq!(
    mem::size_of::<SlottedPageMeta>(),
    mem::size_of::<ArchivedSlottedPageMeta>()
);

impl<'a, T> SlottedPageViewMut<'a, T> {
    /// `prefix_size` is the size of the page header and the page-specific header` (and anything else that comes before the slots)
    /// slot offsets are relative to the initla value of `free_start`
    async fn init(buf: &'a mut [u8], prefix_size: u16) -> nsql_serde::Result<()> {
        let free_start = prefix_size + sizeof!(SlottedPageMeta);
        let header = SlottedPageMeta {
            free_start,
            free_end: PAGE_DATA_SIZE as u16 - free_start,
            slot_len: 0,
        };
        let bytes = nsql_rkyv::archive(&header);
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(())
    }

    /// Safety: `buf` must point at the start of a valid slotted page
    async unsafe fn create(buf: &'a mut [u8]) -> nsql_serde::Result<SlottedPageViewMut<'a, T>> {
        let (header_bytes, buf) =
            buf.split_array_mut::<{ mem::size_of::<ArchivedSlottedPageMeta>() }>();
        let header =
            unsafe { nsql_rkyv::unarchive_root_mut::<SlottedPageMeta>(Pin::new(header_bytes)) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Slot>());

        let slots =
            unsafe { slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Ok(Self { header, slots, data, marker: PhantomData })
    }
}

impl<'a, T: Serialize> SlottedPageViewMut<'a, T> {
    async fn insert(&mut self, value: T) -> nsql_serde::Result<()> {
        // FIXME check there is sufficient space
        // TODO check whether key already exists
        let length = value.serialized_size().await?;
        if length + sizeof!(ArchivedSlot) > self.header.free_end - self.header.free_start {
            todo!()
        }

        let start = self.header.free_end.value() as usize - length as usize;
        value.serialize_into(&mut self.data[start..self.header.free_end.value() as usize]).await?;
        self.header.free_end -= length;
        self.header.slot_len += 1;
        unsafe {
            // write the new slot at the conceptual end of the slot array and recreate the slice to include it
            ptr::write(
                self.slots.as_mut_ptr().add(self.slots.len()),
                Slot { offset: self.header.free_end },
            );
            self.slots = slice::from_raw_parts_mut(self.slots.as_mut_ptr(), self.slots.len() + 1);
        }

        self.header.free_start += sizeof!(ArchivedSlot);
        Ok(())
    }
}
