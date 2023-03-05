use arrayvec::ArrayVec;
use nsql_pager::PageIndex;
use nsql_serde::{AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, Serializer};

use crate::Result;

#[derive(Debug)]
pub(crate) struct Node<K, V, const B: usize>
where
    [K; B]: Sized,
{
    kind: NodeKind<K, V, B>,
    flags: Flags,
}

bitflags::bitflags! {
    #[derive(Debug)]
    pub struct Flags: u8 {
        const IS_ROOT = 1 << 0;
        const IS_LEAF = 1 << 1;
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

impl<K, V, const B: usize> Node<K, V, B>
where
    [K; B]: Sized,
{
    pub(crate) fn new_leaf(leaf: LeafNode<K, V>) -> Self {
        Self { kind: NodeKind::Leaf(leaf), flags: Flags::IS_LEAF }
    }

    pub(crate) fn new_root() -> Self {
        Self { kind: NodeKind::Leaf(LeafNode::default()), flags: Flags::IS_LEAF | Flags::IS_ROOT }
    }

    pub(crate) fn kind(&self) -> &NodeKind<K, V, B> {
        &self.kind
    }

    pub(crate) fn kind_mut(&mut self) -> &mut NodeKind<K, V, B> {
        &mut self.kind
    }

    pub(crate) fn flags(&self) -> &Flags {
        &self.flags
    }
}

impl<K, V, const B: usize> Serialize for Node<K, V, B>
where
    [K; B]: Sized,
    K: Serialize,
    V: Serialize,
{
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), ::std::io::Error> {
        self.flags.serialize(ser).await?;
        match &self.kind {
            NodeKind::Internal(node) => node.serialize(ser).await,
            NodeKind::Leaf(node) => node.serialize(ser).await,
        }
    }
}

impl<K, V, const B: usize> Deserialize for Node<K, V, B>
where
    [K; B]: Sized,
    K: Deserialize,
    V: Deserialize,
{
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, ::std::io::Error> {
        let flags = Flags::deserialize(de).await?;
        let kind = if flags.contains(Flags::IS_LEAF) {
            NodeKind::Leaf(LeafNode::deserialize(de).await?)
        } else {
            NodeKind::Internal(InternalNode::deserialize(de).await?)
        };
        Ok(Self { kind, flags })
    }
}

#[derive(Debug)]
pub(crate) enum NodeKind<K, V, const B: usize>
where
    [K; B]: Sized,
{
    Internal(InternalNode<K, B>),
    Leaf(LeafNode<K, V>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InternalNode<K, const B: usize>
where
    [K; B]: Sized,
{
    keys: ArrayVec<K, B>,
    // FIXME actually should be only B-1 used but run into issues with rustc and const generics if we try to do arithmetic
    children: ArrayVec<PageIndex, B>,
}

impl<K, const B: usize> InternalNode<K, B>
where
    [K; B]: Sized,
{
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl<K: Ord, V: Clone> LeafNode<K, V> {
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

impl<K, V> Default for LeafNode<K, V> {
    fn default() -> Self {
        Self { keys: Default::default(), values: Default::default(), prev: None, next: None }
    }
}
