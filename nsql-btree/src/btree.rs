#![allow(dead_code)]
// just to avoid warnings for now

use std::io;

use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;

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

enum Node<K, V> {
    Internal(InternalNode<K>),
    Leaf(Vec<(K, V)>),
}

struct InternalNode<K> {
    keys: Vec<K>,
    children: Vec<PageIndex>,
}

struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}
