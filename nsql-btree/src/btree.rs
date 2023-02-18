#![allow(dead_code)]
// just to avoid warnings for now

use std::io;

use nsql_buffer::BufferPool;
use nsql_pager::PageIndex;

pub struct BTree<P, K, V> {
    pool: BufferPool<P>,
    root: Node<K, V>,
}

impl<P, K, V> BTree<P, K, V> {
    pub fn new(pool: BufferPool<P>) -> io::Result<Self> {
        Ok(Self { pool, root: todo!() })
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
