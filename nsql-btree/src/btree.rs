#![allow(dead_code)]
// just to avoid warnings for now

use std::io;

use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, Serialize};

pub struct BTree<K, V> {
    pool: BufferPool,
    root: Node<K, V>,
}

impl<K, V> BTree<K, V> {
    pub fn new(_pool: BufferPool) -> io::Result<Self> {
        todo!()
        // Ok(Self { pool, root: todo!() })
    }
}

struct Key(Vec<u8>);
struct Value(Vec<u8>);

// #[derive(Debug, Serialize, Deserialize)]
enum Node<K, V> {
    // #[serde(tag = 0x01u8)]
    Internal(InternalNode<K>),
    Leaf(Vec<(K, V)>),
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalNode<K> {
    keys: Vec<K>,
    children: Vec<PageIndex>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}
