#![allow(dead_code)]
// just to avoid warnings for now

use std::io;

use nsql_buffer::BufferPool;

use crate::node::Node;

pub struct BTree<K, V, const B: usize> {
    pool: BufferPool,
    root: Node<K, V, B>,
}

impl<K, V, const B: usize> BTree<K, V, B> {
    pub fn new(_pool: BufferPool) -> io::Result<Self> {
        todo!()
        // Ok(Self { pool, root: todo!() })
    }
}

struct Key(Vec<u8>);
struct Value(Vec<u8>);
