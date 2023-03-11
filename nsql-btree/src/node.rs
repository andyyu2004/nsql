use nsql_pager::PageIndex;
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, DeserializeWith, Deserializer, Serialize,
    SerializeWith, Serializer,
};

use crate::page::{DeserializeContext, InteriorPage, LeafPage};
use crate::Result;

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
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), ::std::io::Error> {
        ser.write_u8(self.bits()).await
    }
}

impl Deserialize for Flags {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, ::std::io::Error> {
        Ok(Self::from_bits(de.read_u8().await?).unwrap())
    }
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
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), ::std::io::Error> {
        let ctx = DeserializeContext::new(self.flags);
        self.flags.serialize(ser).await?;
        match &self.kind {
            NodeKind::Internal(node) => node.serialize_with(&ctx, ser).await,
            NodeKind::Leaf(node) => node.serialize(ser).await,
        }
    }
}

impl<K, V> Deserialize for Node<K, V>
where
    K: Deserialize,
    V: Deserialize,
{
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, ::std::io::Error> {
        let flags = Flags::deserialize(de).await?;
        let ctx = DeserializeContext::new(flags);
        let kind = if flags.contains(Flags::IS_LEAF) {
            NodeKind::Leaf(LeafPage::deserialize(de).await?)
        } else {
            NodeKind::Internal(InteriorPage::deserialize_with(&ctx, de).await?)
        };
        Ok(Self { kind, flags })
    }
}

#[derive(Debug)]
pub(crate) enum NodeKind<K, V> {
    Internal(InteriorPage<K>),
    Leaf(LeafPage<K, V>),
}
