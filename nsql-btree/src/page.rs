use std::{io};

use nsql_arena::Arena;
use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, SerializeSized, Serializer,
    SliceDeExt, SliceSerExt,
};


use crate::node::Node;

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
        let ser = &mut ser.limit(PAGE_DATA_SIZE);
        self.header.serialize(ser).await?;
        self.slots.serialize(ser).await?;

        for _ in 0..self.header.free_space {
            ser.write_u8(0).await?;
        }

        self.keys.noninline_len().serialize(ser).await?;
        self.children.noninline_len().serialize(ser).await?;

        Ok(())
    }
}

impl<K: Deserialize> Deserialize for InteriorPage<K> {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        let header = InteriorPageHeader::deserialize(de).await?;
        let slots = Arena::deserialize(de).await?;

        for _ in 0..header.free_space {
            assert_eq!(de.read_u8().await?, 0);
        }

        let keys = Vec::deserialize_noninline_len(de, slots.len()).await?;
        let next_pointers = Vec::deserialize_noninline_len(de, slots.len()).await?;

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

#[derive(Debug, PartialEq, SerializeSized)]
struct InteriorPageHeader {
    magic: [u8; 4],
    free_space: u16,
}

impl Deserialize for InteriorPageHeader {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        let mut magic = [0; 4];
        de.read_exact(&mut magic).await?;

        if magic != BTREE_INTERIOR_PAGE_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic for interior btree page",
            ))?;
        }

        let free_space = de.read_u16().await?;

        Ok(Self { magic, free_space })
    }
}

#[derive(Debug, PartialEq, SerializeSized)]
struct LeafPageHeader {
    magic: [u8; 4],
    free_space: u16,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl Deserialize for LeafPageHeader {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        let mut magic = [0; 4];
        de.read_exact(&mut magic).await?;

        if magic != BTREE_LEAF_PAGE_MAGIC {
            Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic for leaf btree page"))?;
        }

        let free_space = de.read_u16().await?;
        let prev = Option::deserialize(de).await?;
        let next = Option::deserialize(de).await?;

        Ok(Self { magic, free_space, prev, next })
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

    pub(crate) fn insert(&mut self, key: K, _value: V) -> Option<V> {
        match self.keys.binary_search(&key) {
            Ok(_i) => todo!(),
            Err(j) => {
                self.keys.insert(j, key);
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
                // remaining space - header size - 4 bytes for the slot length
                free_space: (Node::REMAINING_SPACE - LeafPageHeader::SERIALIZED_SIZE - 4) as u16,
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
        let ser = &mut ser.limit(Node::REMAINING_SPACE);
        self.header.serialize(ser).await?;
        self.slots.serialize(ser).await?;
        for _ in 0..self.header.free_space {
            ser.write_u8(0).await?;
        }

        assert_eq!(self.keys.len(), self.values.len());
        for (k, v) in self.keys.iter().zip(self.values.iter()) {
            k.serialize(ser).await?;
            v.serialize(ser).await?;
        }

        Ok(())
    }
}

impl<K: Deserialize, V: Deserialize> Deserialize for LeafPage<K, V> {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        let header = LeafPageHeader::deserialize(de).await?;
        let slots = Vec::deserialize(de).await?;
        for _ in 0..header.free_space {
            assert_eq!(de.read_u8().await?, 0);
        }

        let n = slots.len();
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            keys.push(K::deserialize(de).await?);
            values.push(V::deserialize(de).await?);
        }

        Ok(Self { header, slots, keys, values })
    }
}
