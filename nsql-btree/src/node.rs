use std::mem;

use nsql_pager::PAGE_DATA_SIZE;
use nsql_serde::{AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, Serializer};

use crate::page::{InteriorPage, LeafPage};

#[derive(Debug)]
pub(crate) struct Node<K, V> {
    kind: NodeKind<K, V>,
    flags: Flags,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct Flags: u8 {
        const IS_ROOT = 1 << 0;
        const IS_LEAF = 1 << 1;
        const VARIABLE_SIZE_KEYS = 1 << 2;
    }
}

impl Serialize for Flags {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        Ok(ser.write_u8(self.bits()).await?)
    }
}

impl Deserialize for Flags {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        Ok(Self::from_bits(de.read_u8().await?).unwrap())
    }
}

impl Node<(), ()> {
    /// amount of space available on the rest of the page
    pub(crate) const REMAINING_SPACE: usize = PAGE_DATA_SIZE - mem::size_of::<Flags>();
}

impl<K, V> Node<K, V> {
    pub(crate) fn new_leaf(leaf: LeafPage<K, V>) -> Self {
        Self { kind: NodeKind::Leaf(leaf), flags: Flags::IS_LEAF }
    }

    pub(crate) fn new_root() -> Self {
        Self { kind: NodeKind::Leaf(LeafPage::default()), flags: Flags::IS_LEAF | Flags::IS_ROOT }
    }

    pub(crate) fn kind(&self) -> &NodeKind<K, V> {
        &self.kind
    }

    pub(crate) fn kind_mut(&mut self) -> &mut NodeKind<K, V> {
        &mut self.kind
    }

    pub(crate) fn flags(&self) -> &Flags {
        &self.flags
    }
}

impl<K, V> Serialize for Node<K, V>
where
    K: Serialize,
    V: Serialize,
{
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        let ser = &mut ser.limit(PAGE_DATA_SIZE);
        self.flags.serialize(ser).await?;
        match &self.kind {
            NodeKind::Internal(node) => node.serialize(ser).await,
            NodeKind::Leaf(node) => node.serialize(ser).await,
        }
    }
}

impl<K, V> Deserialize for Node<K, V>
where
    K: Deserialize,
    V: Deserialize,
{
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> nsql_serde::Result<Self> {
        let flags = Flags::deserialize(de).await?;
        let kind = if flags.contains(Flags::IS_LEAF) {
            NodeKind::Leaf(LeafPage::deserialize(de).await?)
        } else {
            NodeKind::Internal(InteriorPage::deserialize(de).await?)
        };
        Ok(Self { kind, flags })
    }
}

#[derive(Debug)]
pub(crate) enum NodeKind<K, V> {
    Internal(InteriorPage<K>),
    Leaf(LeafPage<K, V>),
}
