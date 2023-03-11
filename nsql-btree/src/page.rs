use std::io;
use std::marker::PhantomData;

use nsql_arena::Arena;
use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, SerializeSized, Serializer,
    SliceDeExt, SliceSerExt,
};

use crate::node::{Flags, Node};

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";
const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

#[derive(Debug)]
pub(crate) struct InteriorPage<K> {
    header: InteriorPageHeader,
    slots: Arena<Slot>,
    keys: Vec<K>,
    children: Vec<PageIndex>,
}

impl<K> InteriorPage<K> {
    pub(crate) fn search(&self, key: &K) -> PageIndex
    where
        K: Ord,
    {
        match self.keys.binary_search(key) {
            Ok(i) => self.children[i],
            Err(i) => self.children[i],
        }
    }
}

impl<K: Serialize> Serialize for InteriorPage<K> {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        let ser = &mut ser.limit(Node::PAGE_SIZE);
        self.header.serialize(ser).await?;
        self.slots.serialize(ser).await?;

        ser.fill(self.header.free_space).await?;

        self.keys.noninline_len().serialize(ser).await?;
        self.children.noninline_len().serialize(ser).await?;

        Ok(())
    }
}

impl<K: Deserialize> Deserialize for InteriorPage<K> {
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let header = InteriorPageHeader::deserialize(de).await?;
        let slots = Arena::deserialize(de).await?;

        de.skip_fill(header.free_space).await?;

        let keys = Vec::deserialize_noninline_len(de, slots.len()).await?;
        let next_pointers = Vec::deserialize_noninline_len(de, slots.len()).await?;

        Ok(Self { header, slots, keys, children: next_pointers })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, SerializeSized, Deserialize)]
struct Slot {
    /// The offset of the entry from the start of the page
    offset: u16,
}

#[derive(Debug, PartialEq, SerializeSized)]
struct InteriorPageHeader {
    magic: [u8; 4],
    free_space: u16,
}

impl Deserialize for InteriorPageHeader {
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let mut magic = [0; 4];
        de.read_exact(&mut magic).await?;

        if magic != BTREE_INTERIOR_PAGE_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid magic for interior btree page: expected {BTREE_INTERIOR_PAGE_MAGIC:?}, got {magic:?}",
                ),
            ))?;
        }

        let free_space = de.read_u16().await?;

        Ok(Self { magic, free_space })
    }
}

#[derive(Debug, PartialEq, SerializeSized)]
struct LeafPageHeader {
    magic: [u8; 4],
    free_start: u16,
    free_end: u16,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl LeafPageHeader {
    pub(crate) fn free_space(&self) -> u16 {
        self.free_end - self.free_start
    }
}

impl Deserialize for LeafPageHeader {
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let mut magic = [0; 4];
        de.read_exact(&mut magic).await?;

        if magic != BTREE_LEAF_PAGE_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid magic for leaf btree page: expected {BTREE_LEAF_PAGE_MAGIC:?}, got {magic:?}"
                ),
            ))?;
        }

        let free_start = de.read_u16().await?;
        let free_end = de.read_u16().await?;
        let prev = Option::deserialize(de).await?;
        let next = Option::deserialize(de).await?;

        Ok(Self { magic, free_start, free_end, prev, next })
    }
}

#[derive(Debug)]
pub(crate) struct LeafPage<K, V> {
    header: LeafPageHeader,
    slots: Vec<Slot>,
    keys: Vec<K>,
    values: Vec<V>,
}

impl<K: Ord, V: Clone> LeafPage<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.keys.binary_search(key).ok().map(|i| self.values[i].clone())
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.keys.binary_search(&key) {
            Ok(_i) => todo!(),
            Err(j) => {
                self.keys.insert(j, key);
                self.values.insert(j, value);
                None
            }
        }
    }
}

impl<K, V> Default for LeafPage<K, V> {
    fn default() -> Self {
        Self {
            header: LeafPageHeader {
                magic: BTREE_LEAF_PAGE_MAGIC,
                // header + the slot length
                free_start: LeafPageHeader::SERIALIZED_SIZE + 4,
                free_end: Node::PAGE_SIZE,
                prev: None,
                next: None,
            },
            slots: Default::default(),
            keys: Default::default(),
            values: Default::default(),
        }
    }
}

impl<K: Serialize, V: Serialize> Serialize for LeafPage<K, V> {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        let ser = &mut ser.limit(Node::PAGE_SIZE);
        self.header.serialize(ser).await?;
        self.slots.serialize(ser).await?;

        ser.fill(self.header.free_space()).await?;

        assert_eq!(self.keys.len(), self.values.len());
        for (k, v) in self.keys.iter().zip(self.values.iter()) {
            k.serialize(ser).await?;
            v.serialize(ser).await?;
        }

        Ok(())
    }
}

#[derive(Debug, SerializeSized, Deserialize)]
pub(crate) struct PageHeader {
    pub(crate) flags: Flags,
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
        let mut de = &data[PageHeader::SERIALIZED_SIZE as usize..];
        let header = LeafPageHeader::deserialize(&mut de).await?;
        let slots = Vec::deserialize(&mut de).await?;
        Ok(Self { header, data, slots, marker: std::marker::PhantomData })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        for slot in self.slots.iter() {
            let offset = slot.offset as usize;
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
    pub(crate) async fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageViewMut<'a, K, V>> {
        let mut de = &data[..];
        let header = PageHeader::deserialize(&mut de).await?;
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageViewMut::create(data).await.map(Self::Leaf)
        } else {
            todo!()
        }
    }
}

pub(crate) struct LeafPageViewMut<'a, K, V> {
    header: LeafPageHeader,
    slot_idx: usize,
    data: &'a mut [u8; PAGE_DATA_SIZE],
    marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    pub(crate) async fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let mut de = &data[PageHeader::SERIALIZED_SIZE as usize..];
        let header = LeafPageHeader::deserialize(&mut de).await?;
        let slot_idx = de.read_u32().await? as usize;
        Ok(Self { header, data, slot_idx, marker: std::marker::PhantomData })
    }
}

impl<'a, K: Serialize, V: Serialize> LeafPageViewMut<'a, K, V> {
    pub(crate) async fn get(&self, key: &K) -> Option<V> {
        todo!()
    }

    pub(crate) async fn insert(&mut self, key: K, value: V) -> nsql_serde::Result<Option<V>> {
        assert_eq!(self.slot_idx, 0, "todo");
        // FIXME check there is sufficient space
        // TODO check whether key already exists
        let length = value.serialized_size().await?;
        if length + Slot::SERIALIZED_SIZE > self.header.free_space() {
            todo!()
        }

        // FIXME try to rkyv or somethign to get a view into stuff

        1u32.serialize_into(&mut self.data[1 + LeafPageHeader::SERIALIZED_SIZE as usize..]).await?;
        self.header.free_end -= length;
        Slot { offset: self.header.free_start }
            .serialize_into(&mut self.data[self.header.free_start as usize..])
            .await?;
        self.header.free_start += Slot::SERIALIZED_SIZE;
        Ok(None)
    }
}

struct SlottedPageView<'a, H, T> {
    header: H,
    free_start: u16,
    free_end: u16,
    slots: &'a [Slot],
    data: &'a [u8; PAGE_DATA_SIZE],
    marker: PhantomData<T>,
}

impl<'a, H, T: Deserialize> SlottedPageView<'a, H, T> {
    pub(crate) async fn get(&mut self, idx: u16) -> nsql_serde::Result<T> {
        let slot = self.slots[idx as usize];
        let offset = slot.offset as usize;
        Ok(T::deserialize(&mut &self.data[offset..]).await?)
        // // FIXME check there is sufficient space
        // // TODO check whether key already exists
        // let length = value.serialized_size().await?;
        // if length + Slot::SERIALIZED_SIZE > self.header.free_space() {
        //     todo!()
        // }

        // // FIXME try to rkyv or somethign to get a view into stuff

        // 1u32.serialize_into(&mut self.data[1 + LeafPageHeader::SERIALIZED_SIZE as usize..]).await?;
        // self.header.free_end -= length;
        // Slot { offset: self.header.free_end, length }
        //     .serialize_into(&mut self.data[self.header.free_start as usize..])
        //     .await?;
        // self.header.free_start += Slot::SERIALIZED_SIZE;
        // Ok(None)
    }
}
