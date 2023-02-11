use std::io;

use nsql_pager::SingleFilePager;

pub struct BTree {
    pager: SingleFilePager,
}

impl BTree {
    pub fn open() -> io::Result<Self> {
        todo!()
        // let pager = SingleFilePager::open(path)?;
        // Ok(Self { pager })
    }
}

struct Offset(usize);

struct Key(Vec<u8>);
struct Value(Vec<u8>);

struct Node {
    node_type: NodeType,
}

enum NodeType {
    Internal(Vec<Offset>, Vec<Key>),
    Leaf(Vec<(Key, Value)>),
}
