use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, Serialize};

// #[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Node<K, V, const B: usize> {
    // #[serde(tag = 0x01u8)]
    // Internal(InternalNode<K, B>),
    Leaf(LeafNode<K, V>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InternalNode<K, const B: usize>
where
    [K; B + 1]: Sized,
{
    keys: [K; B + 1],
    children: [K; B],
}

#[derive(Debug, Serialize)]
pub(crate) struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}
