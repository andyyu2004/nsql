use std::marker::PhantomData;
use std::pin::Pin;
use std::{fmt, mem, slice};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{Deserialize, Serialize, SerializeSized};
use rkyv::rend::BigEndian;
use rkyv::Archive;

use crate::node::Flags;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";
const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

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
    pub(crate) async fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageView<'a, K, V>> {
        let header = PageHeader::deserialize(&mut &data[..]).await?;
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageView::create(data).await.map(Self::Leaf)
        } else {
            todo!()
        }
    }
}

pub(crate) struct LeafPageView<'a, K, V> {
    header: LeafPageHeader,
    slots: Vec<Slot>,
    data: &'a [u8; PAGE_DATA_SIZE],
    marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Ord + Deserialize,
    V: Deserialize,
{
    pub(crate) async fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let _de = &data[PageHeader::SERIALIZED_SIZE as usize..];
        todo!();
        // let header = LeafPageHeader::deserialize(&mut de).await?;
        // let slots = Vec::deserialize(&mut de).await?;
        // Ok(Self { header, data, slots, marker: std::marker::PhantomData })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        for slot in self.slots.iter() {
            let offset = slot.offset.value() as usize;
            let mut de = &self.data[offset..];
            let k = K::deserialize(&mut de).await?;
            // FIXME better format so we don't have to deserialize values too
            let v = V::deserialize(&mut de).await.ok();
            if k == *key {
                return Ok(v);
            }
        }
        todo!()
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
    view: SlottedPageViewMut<'a, (K, V)>,
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<()> {
        let header_bytes = nsql_rkyv::archive(&LeafPageHeader::default());
        data[0..header_bytes.len()].copy_from_slice(&header_bytes);
        // the slots start after the page header and the leaf page header
        const HEADER_SIZE: u16 = mem::size_of::<ArchivedLeafPageHeader>() as u16;
        let slot_start = PageHeader::SERIALIZED_SIZE + HEADER_SIZE;
        SlottedPageViewMut::<(K, V)>::init(&mut data[HEADER_SIZE as usize..], slot_start).await?;

        Ok(())
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_mut::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = nsql_rkyv::unarchive_root_mut::<LeafPageHeader>(Pin::new(header_bytes));
        let view = SlottedPageViewMut::create(data).await?;
        Ok(Self { header, view })
    }
}

impl<'a, K: Serialize, V: Serialize> LeafPageViewMut<'a, K, V> {
    pub(crate) async fn insert(&self, _key: K, _value: V) -> nsql_serde::Result<Option<V>> {
        dbg!(&self.view.header);
        dbg!(&self.view.slots);
        todo!()
    }
}

struct SlottedPageView<'a, H, T> {
    header: &'a SlottedPageHeader,
    slots: &'a [Slot],
    data: &'a [u8; PAGE_DATA_SIZE],
    marker: PhantomData<T>,
    page_header: PhantomData<H>,
}

impl<'a, H: SerializeSized, T: Deserialize> SlottedPageView<'a, H, T> {
    /// Safety: `data` must be a valid slotted page with the correct header type
    async unsafe fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<SlottedPageView<'a, H, T>> {
        let buf = &data[(PageHeader::SERIALIZED_SIZE + H::SERIALIZED_SIZE) as usize..];
        let header = unsafe { &*(buf as *const _ as *const SlottedPageHeader) };
        let slots =
            unsafe { slice::from_raw_parts(buf.as_ptr() as *const Slot, header.slot_len as usize) };
        Ok(Self { header, slots, data, marker: PhantomData, page_header: PhantomData })
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
    header: Pin<&'a mut ArchivedSlottedPageHeader>,
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
struct SlottedPageHeader {
    free_start: u16,
    free_end: u16,
    slot_len: u16,
}

nsql_util::static_assert_eq!(
    mem::size_of::<SlottedPageHeader>(),
    mem::size_of::<ArchivedSlottedPageHeader>()
);

impl<'a, T> SlottedPageViewMut<'a, T> {
    async fn init(buf: &'a mut [u8], free_start: u16) -> nsql_serde::Result<()> {
        let header = SlottedPageHeader { free_start, free_end: PAGE_DATA_SIZE as u16, slot_len: 0 };
        let bytes = nsql_rkyv::archive(&header);
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(())
    }

    /// Safety: `buf` must point at the start of a valid slotted page
    async unsafe fn create(buf: &'a mut [u8]) -> nsql_serde::Result<SlottedPageViewMut<'a, T>> {
        let (header_bytes, buf) =
            buf.split_array_mut::<{ mem::size_of::<ArchivedSlottedPageHeader>() }>();
        let header =
            unsafe { nsql_rkyv::unarchive_root_mut::<SlottedPageHeader>(Pin::new(header_bytes)) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, rest) = buf.split_at_mut(slot_len * mem::size_of::<Slot>());

        let slots =
            unsafe { slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Ok(Self { header, slots, data: rest, marker: PhantomData })
    }
}

impl<'a, T: Serialize> SlottedPageViewMut<'a, T> {
    async fn insert(&mut self, _value: T) -> nsql_serde::Result<()> {
        todo!()
        // FIXME check there is sufficient space
        // TODO check whether key already exists
        // let length = value.serialized_size().await?;
        // if length + Slot::SERIALIZED_SIZE > self.free_space() {
        //     todo!()
        // }

        // // FIXME try to rkyv or somethign to get a view into stuff

        // 1u32.serialize_into(&mut self.data[1 + LeafPageHeader::SERIALIZED_SIZE as usize..]).await?;
        // self.free_end -= length;
        // Slot { offset: self.free_end }
        //     .serialize_into(&mut self.data[self.header.free_start as usize..])
        //     .await?;
        // self.header.free_start += Slot::SERIALIZED_SIZE;
        // Ok(None)
    }
}
