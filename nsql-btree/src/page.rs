use std::io;

use nsql_arena::Arena;
use nsql_pager::PageIndex;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, DeserializeWith, Deserializer, Serialize,
    SerializeWith, Serializer,
};

use crate::node::Flags;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";

#[derive(Debug)]
pub(crate) struct InteriorPage<K> {
    header: BTreePageHeader,
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

pub(super) struct DeserializeContext {
    flags: Flags,
}

impl DeserializeContext {
    pub(super) fn new(flags: Flags) -> Self {
        Self { flags }
    }
}

impl<K: Serialize> SerializeWith for InteriorPage<K> {
    type Context<'a> = DeserializeContext;

    async fn serialize_with(
        &self,
        _ctx: &Self::Context<'_>,
        ser: &mut dyn Serializer<'_>,
    ) -> Result<(), io::Error> {
        self.header.serialize(ser).await?;
        self.slots.serialize(ser).await?;

        for _ in 0..self.header.free_space {
            ser.write_u8(0).await?;
        }

        self.keys.serialize(ser).await?;
        self.children.serialize(ser).await?;

        Ok(())
    }
}

impl<K: Deserialize> DeserializeWith for InteriorPage<K> {
    type Context<'a> = DeserializeContext;

    async fn deserialize_with(
        _ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, io::Error> {
        let header = BTreePageHeader::deserialize(de).await?;
        let slots = Arena::deserialize(de).await?;

        for _ in 0..header.free_space {
            assert_eq!(de.read_u8().await?, 0);
        }

        let keys = Vec::deserialize(de).await?;
        let next_pointers = Vec::deserialize(de).await?;

        Ok(Self { header, slots, keys, children: next_pointers })
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Slot {
    /// The offset of the entry from the start of the page
    offset: u16,
    /// The length of the entry
    length: u16,
}

#[derive(Debug, PartialEq, Serialize)]
struct BTreePageHeader {
    magic: [u8; 4],
    free_space: u16,
}

impl Deserialize for BTreePageHeader {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, ::std::io::Error> {
        let mut magic = [0; 4];
        de.read_exact(&mut magic).await?;

        if magic != BTREE_INTERIOR_PAGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic for interior btree page",
            ));
        }

        let free_space = de.read_u16().await?;

        Ok(Self { magic, free_space })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LeafPage<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl<K: Ord, V: Clone> LeafPage<K, V> {
    pub(crate) fn get(&self, key: &K) -> Option<V> {
        self.keys.binary_search(key).ok().map(|i| self.values[i].clone())
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.keys.binary_search(&key) {
            Ok(i) => {
                todo!()
            }
            Err(j) => {
                self.keys.insert(j, key);
                None
            }
        }
    }
}

impl<K, V> Default for LeafPage<K, V> {
    fn default() -> Self {
        Self { keys: Default::default(), values: Default::default(), prev: None, next: None }
    }
}
