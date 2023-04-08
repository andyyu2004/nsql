//! In memory implementation

use std::sync::Arc;

use parking_lot::RwLock;

pub struct BTree<K, V, const B: usize> {
    root: Link<K, V, B>,
}

impl<K: Ord, V: Clone, const B: usize> BTree<K, V, B> {
    pub fn get(&self, key: &K) -> Option<V> {
        let root = self.root.read();
        match &*root {
            Node::Leaf(leaf) => leaf.get(key).cloned(),
            Node::Interior(internal) => todo!(),
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut root = self.root.write();
        match &mut *root {
            Node::Leaf(leaf) => match leaf.insert(key, value) {
                Ok(prev) => prev,
                Err(NodeFull) => {
                    cov_mark::hit!(test_mem_leaf_split);
                    let new_root = Interior::new();
                    None
                }
            },
            Node::Interior(internal) => todo!(),
        }
    }
}

impl<K, V, const B: usize> Clone for BTree<K, V, B> {
    fn clone(&self) -> Self {
        Self { root: Arc::clone(&self.root) }
    }
}

impl<K, V, const B: usize> Default for BTree<K, V, B> {
    fn default() -> Self {
        assert!(B >= 3, "B must be at least 3");
        Self { root: Arc::new(RwLock::new(Node::Leaf(Leaf::default()))) }
    }
}

enum Node<K, V, const B: usize> {
    Leaf(Leaf<K, V, B>),
    Interior(Interior<K, V, B>),
}

#[derive(Debug)]
struct NodeFull;

struct Leaf<K, V, const B: usize> {
    entries: Vec<(K, V)>,
}

impl<K: Ord, V, const B: usize> Leaf<K, V, B> {
    fn get(&self, key: &K) -> Option<&V> {
        self.entries.binary_search_by(|(k, _)| k.cmp(key)).ok().map(|index| &self.entries[index].1)
    }

    fn insert(&mut self, key: K, value: V) -> Result<Option<V>, NodeFull> {
        match self.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(index) => {
                let (_, old_value) = self.entries.swap_remove(index);
                Ok(Some(old_value))
            }
            Err(index) => {
                if self.entries.len() == B {
                    Err(NodeFull)
                } else {
                    self.entries.insert(index, (key, value));
                    Ok(None)
                }
            }
        }
    }
}

impl<K, V, const B: usize> Default for Leaf<K, V, B> {
    fn default() -> Self {
        Self { entries: Default::default() }
    }
}

struct Interior<K, V, const B: usize> {
    leftmost_child: Link<K, V, B>,
    high_key: Option<K>,
    entries: Vec<(K, Link<K, V, B>)>,
}

pub type Link<K, V, const B: usize> = Arc<RwLock<Node<K, V, B>>>;

impl<K, V, const B: usize> Interior<K, V, B> {
    fn new(
        leftmost_child: Link<K, V, B>,
        sep: K,
        value: Link<K, V, B>,
        high_key: Option<K>,
    ) -> Self {
        Self { leftmost_child, high_key, entries: vec![(sep, value)] }
    }
}

// struct IdealInterior<K, V, const B: usize> {
//     keys: Vec<K>,
//     children: Vec<V>,
// }
//
// impl<K, V, const B: usize> IdealInterior<K, V, B> {
//     fn new(low_value: V, sep: K, value: V, high_key: Option<K>) -> Self {
//         // now how to do something like this on our disk format?
//         match high_key {
//             Some(high_key) => Self { keys: vec![sep, high_key], children: vec![low_value, value] },
//             None => Self { keys: vec![sep], children: vec![low_value, value] },
//         }
//     }
// }

#[cfg(test)]
mod tests;
