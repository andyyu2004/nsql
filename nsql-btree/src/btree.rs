use std::marker::PhantomData;
use std::mem;

use async_recursion::async_recursion;
use nsql_buffer::BufferPool;
use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{Deserialize, FixedSizeSerialize, Serialize};
use nsql_util::static_assert;

use crate::node::{Flags, InternalNode, Node, NodeKind};
use crate::Result;

/// A B+ tree
pub struct BTree<K: FixedSizeSerialize, V>
where
    [K; b(K::SIZE)]: Sized,
{
    pool: BufferPool,
    root_idx: PageIndex,
    marker: std::marker::PhantomData<fn() -> (K, V)>,
}

// compute branching factor `B` from key size (and page size)
#[doc(hidden)]
pub const fn b(key_size: usize) -> usize {
    // derived from rearranging
    // should really be (B-1) * key_size but we're using B for now due to rustc bugs see
    // page_data_size >= B * key_size + B * mem::size_of::<PageIndex>() + 1 + 8 (+ 1 for the flags byte, 4 * 2 for array lengths)
    let b = (PAGE_DATA_SIZE - 8 - 1) / (key_size + mem::size_of::<PageIndex>());
    assert!(b > 1);
    b
}

static_assert!(PAGE_DATA_SIZE >= mem::size_of::<InternalNode<u64, { b(8) }>>());

impl<K, V> BTree<K, V>
where
    K: Ord,
    [K; b(K::SIZE)]: Sized,
    K: FixedSizeSerialize + Deserialize,
    V: Serialize + Deserialize + Clone,
{
    pub async fn create(pool: BufferPool) -> Result<Self> {
        let handle = pool.alloc().await?;
        let root = Node::<K, V, { b(K::SIZE) }>::new_root();
        root.serialize(&mut handle.page().data_mut()).await?;
        Ok(Self { pool, root_idx: handle.page().idx(), marker: PhantomData })
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.search_node(self.root_idx, key).await
    }

    async fn root(&self) -> Result<Node<K, V, { b(K::SIZE) }>> {
        let node = self.load_node_at(self.root_idx).await?;
        assert!(node.flags().contains(Flags::IS_ROOT));
        Ok(node)
    }

    #[async_recursion(?Send)]
    async fn search_node(&self, idx: PageIndex, key: &K) -> Result<Option<V>> {
        let node = self.load_node_at(idx).await?;
        match node.kind() {
            NodeKind::Internal(node) => self.search_node(node.search(key), key).await,
            NodeKind::Leaf(leaf) => Ok(leaf.get(key)),
        }
    }

    // FIXME deserializing doesn't work because we will deserialize the same page into multiple
    // instances of nodes. Try use bytemuck somehow
    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        match self.root().await?.kind_mut() {
            NodeKind::Internal(_) => todo!(),
            NodeKind::Leaf(leaf) => Ok(leaf.insert(key, value)),
        }
    }

    async fn load_node_at(&self, idx: PageIndex) -> Result<Node<K, V, { b(K::SIZE) }>> {
        let handle = self.pool.load(idx).await?;
        let mut data = handle.page().data();
        Node::deserialize(&mut data).await
    }
}

struct Key(Vec<u8>);
struct Value(Vec<u8>);
