use std::{io, mem};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, SerializeSized, Serializer,
};

use crate::page::{InteriorPage, LeafPage, PageHeader};

#[derive(Debug)]
pub(crate) struct Node<K, V> {
    kind: NodeKind<K, V>,
    flags: Flags,
}

impl Node<(), ()> {
    /// amount of space available on the rest of the page
    pub(crate) const PAGE_SIZE: u16 = PAGE_DATA_SIZE as u16 - Flags::SERIALIZED_SIZE;
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
        let ser = &mut ser.limit(PAGE_DATA_SIZE as u16);
        PageHeader { flags: self.flags }.serialize(ser).await?;
        match &self.kind {
            NodeKind::Internal(node) => node.serialize(ser).await,
            NodeKind::Leaf(node) => node.serialize(ser).await,
        }
    }
}

#[derive(Debug)]
pub(crate) enum NodeKind<K, V> {
    Internal(InteriorPage<K>),
    Leaf(LeafPage<K, V>),
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
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let bits = de.read_u8().await?;
        match Self::from_bits(bits) {
            Some(flags) => Ok(flags),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree bitflags: {bits:#b}"),
            ))?,
        }
    }
}

impl SerializeSized for Flags {
    const SERIALIZED_SIZE: u16 = mem::size_of::<Self>() as u16;
}
