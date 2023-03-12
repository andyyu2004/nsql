use std::mem;
use std::pin::Pin;

use nsql_pager::{PageOffset, PAGE_DATA_SIZE, PAGE_SIZE};
use nsql_util::{static_assert, static_assert_eq};
use rkyv::Archive;

use crate::table_storage::HeapTuple;

/// A single page in the free space map
#[derive(Debug, Clone, Copy, Archive)]
pub(super) struct FsmPage {
    // page indexes that the leaf node corresponds to
    // this field should come first so the u32s are aligned
    leaves: [u32; LEAF_COUNT as usize],
    nodes: [Bucket; NODES_PER_PAGE as usize],
    flags: u8,
}

// this is the biggest number that can make the structure fit in a page
const NODES_PER_PAGE: u16 = 2455;
static_assert_eq!(PAGE_DATA_SIZE, mem::size_of::<FsmPage>());

impl FsmPage {
    #[inline]
    pub fn from_bytes_mut(bytes: &mut [u8; mem::size_of::<Self>()]) -> Pin<&mut ArchivedFsmPage> {
        unsafe { nsql_rkyv::unarchive_root_mut::<Self>(Pin::new(bytes)) }
    }
}

impl Default for FsmPage {
    fn default() -> Self {
        Self {
            nodes: [Bucket::default(); NODES_PER_PAGE as usize],
            leaves: [0; LEAF_COUNT as usize],
            flags: 0,
        }
    }
}

#[derive(Debug)]
enum Node {
    Internal {
        /// the maximum bucket of its two children
        max: Bucket,
        left: Option<NodeIndex>,
        right: Option<NodeIndex>,
    },
    Leaf {
        offset: PageOffset,
        bucket: Bucket,
    },
}

// this assertion is mostly here to run the internal assertions of the function
static_assert_eq!(FsmPage::size_to_bucket(HeapTuple::MAX_SIZE).value, 255);
static_assert_eq!(FsmPage::size_to_bucket(HeapTuple::MAX_SIZE - 1).value, 254);
static_assert!(FsmPage::size_to_bucket(1).value == 0);
static_assert!(HeapTuple::MAX_SIZE <= PAGE_DATA_SIZE as u16);

type NodeIndex = u16;

/// The index of the root node
const ROOT_NODE_IDX: NodeIndex = 0;

pub(super) const FIRST_LEAF_IDX: NodeIndex = compute_first_leaf_idx(NODES_PER_PAGE);
pub(super) const LEAF_COUNT: NodeIndex = NODES_PER_PAGE - FIRST_LEAF_IDX;
pub(super) const MAX_OFFSET: NodeIndex = LEAF_COUNT - 1;

// this max offset should be in bounds
static_assert!(FIRST_LEAF_IDX + MAX_OFFSET < NODES_PER_PAGE);

/// Given number of nodes in a left-complete binary tree, return the index of the first leaf node
const fn compute_first_leaf_idx(count: NodeIndex) -> NodeIndex {
    // A complete binary tree will have 2^N - 1 nodes
    // we want the following mappings where [x,y] is the inclusive range
    // [1,1]  -> 0
    // [2,3]  -> 1
    // [4,7]  -> 3
    // [8,15] -> 7
    // or more generally
    // [2^N, 2^(N+1) - 1] -> 2^N - 1

    // Consider the following tree to visualize it
    //        0
    //    1       2
    //  3   4   5   6
    // 7 8 9 A B C D E
    static_assert_eq!(compute_first_leaf_idx(1), 0);
    static_assert_eq!(compute_first_leaf_idx(2), 1);
    static_assert_eq!(compute_first_leaf_idx(3), 1);
    static_assert_eq!(compute_first_leaf_idx(6), 3);
    static_assert_eq!(compute_first_leaf_idx(7), 3);
    static_assert_eq!(compute_first_leaf_idx(8), 7);
    static_assert_eq!(compute_first_leaf_idx(15), 7);
    static_assert_eq!(compute_first_leaf_idx(16), 15);
    static_assert_eq!(compute_first_leaf_idx(1023), 511);
    static_assert_eq!(compute_first_leaf_idx(1024), 1023);

    assert!(count > 0);
    if count.is_power_of_two() { count - 1 } else { (count.next_power_of_two() / 2) - 1 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Archive)]
#[archive(as = "Self")]
#[repr(transparent)]
pub(super) struct Bucket {
    value: u8,
}

impl Bucket {
    const fn new(value: u8) -> Self {
        Self { value }
    }

    fn max(self, other: Self) -> Self {
        Self::new(self.value.max(other.value))
    }

    const MAX: Self = Self { value: u8::MAX };

    /// return whether this bucket can satisfy the request size
    fn can_satify(self, other: Self) -> bool {
        // if we are strictly larger then we can definitely satisfy the request
        // non-strict equality is not sufficient as we may be in the same bucket but the size request was actually larger than we have available
        if self.value > other.value {
            return true;
        }

        // otherwise we can only satisfy the request if we are the special case of the max bucket
        self == other && self == Self::MAX
    }
}

impl FsmPage {
    /// Returns the page offset relative to the current fsm page.
    /// The first leaf node is at offset 0, the next leaf node is at offset 1, etc.
    #[inline]
    pub fn find(&self, required_size: u16) -> Option<PageOffset> {
        self.search_node(ROOT_NODE_IDX, Self::size_to_bucket(required_size))
    }

    #[inline]
    pub fn find_and_update(&mut self, size: u16) -> Option<PageOffset> {
        let offset = self.find(size)?;
        self.update(offset, size);
        Some(offset)
    }

    /// update the free space map with the new free space on the page
    #[inline]
    pub fn update(&mut self, offset: PageOffset, free_space: u16) {
        assert!(offset.as_u32() <= MAX_OFFSET as u32);
        assert!(free_space <= HeapTuple::MAX_SIZE);
        let bucket = Self::size_to_bucket(free_space);

        let mut idx = self.page_offset_to_idx(offset);
        // update the value of the appropriate leaf node
        self.nodes[idx as usize] = bucket;
        // recompute the maximum of parents up to the root
        while let Some(parent) = self.parent(idx) {
            let curr = match self.node_at(parent) {
                Node::Internal { max, .. } => max,
                Node::Leaf { .. } => unreachable!(),
            };
            self.nodes[parent as usize] = curr.max(self.nodes[idx as usize]);
            idx = parent;
        }
    }

    const fn size_to_bucket(size: u16) -> Bucket {
        // We allocate one byte to represent the free space on a page.
        // Therefore, the granularity of free space is `PAGE_SIZE / 256` bytes.
        // We round down to the nearest bucket.

        assert!(size > 0);
        assert!(size <= HeapTuple::MAX_SIZE);

        // special case for the maximum size
        if size == HeapTuple::MAX_SIZE {
            return Bucket::MAX;
        }

        let bucket = (size - 1) / (PAGE_SIZE as u16 / 256);
        assert!(bucket <= u8::MAX as u16);
        Bucket::new(bucket as u8)
    }

    fn search_node(&self, idx: NodeIndex, requirement: Bucket) -> Option<PageOffset> {
        match self.node_at(idx) {
            Node::Internal { max, left, right } if max.can_satify(requirement) => {
                // NOTE: this favours the left node, we could do best-fit instead
                if let Some(offset) = self.search_node(left?, requirement) {
                    return Some(offset);
                }

                // FIXME don't panic in this case but try and repair the tree
                let right = self
                    .search_node(right?, requirement)
                    .unwrap_or_else(|| panic!("parent said max `{}` > required `{}`, but neither child had enough free space", max.value, requirement.value));
                Some(right)
            }
            Node::Internal { .. } => None,
            Node::Leaf { bucket, offset } => bucket.can_satify(requirement).then_some(offset),
        }
    }

    fn node_at(&self, idx: NodeIndex) -> Node {
        let value = self.nodes[idx as usize];
        match (self.left_child(idx), self.right_child(idx)) {
            (None, None) => Node::Leaf { bucket: value, offset: self.idx_to_page_offset(idx) },
            (left, right) => Node::Internal { max: value, left, right },
        }
    }

    /// return the apge offset that the leaf node at `idx` represents
    fn idx_to_page_offset(&self, idx: NodeIndex) -> PageOffset {
        assert!(self.is_leaf(idx));
        debug_assert!(self.is_leaf(FIRST_LEAF_IDX));
        assert!(idx >= FIRST_LEAF_IDX);
        PageOffset::new((idx - FIRST_LEAF_IDX) as u32)
    }

    fn page_offset_to_idx(&self, offset: PageOffset) -> NodeIndex {
        debug_assert!(self.is_leaf(FIRST_LEAF_IDX));
        FIRST_LEAF_IDX + offset.as_u32() as NodeIndex
    }

    fn is_leaf(&self, idx: NodeIndex) -> bool {
        let is_leaf = idx > ROOT_NODE_IDX;
        assert!(!is_leaf || self.left_child(idx).is_none() && self.right_child(idx).is_none());
        is_leaf
    }

    fn parent(&self, idx: NodeIndex) -> Option<NodeIndex> {
        if idx == ROOT_NODE_IDX { None } else { Some((idx - 1) / 2) }
    }

    fn left_child(&self, idx: NodeIndex) -> Option<NodeIndex> {
        let child = idx * 2 + 1;
        if child < NODES_PER_PAGE { Some(child) } else { None }
    }

    fn right_child(&self, idx: NodeIndex) -> Option<NodeIndex> {
        let child = idx * 2 + 2;
        if child < NODES_PER_PAGE { Some(child) } else { None }
    }
}
