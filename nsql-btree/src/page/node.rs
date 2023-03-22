use std::fmt;
use std::pin::Pin;

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use rkyv::{Archive, Archived};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{ArchivedKeyValuePair, Flags, InteriorPageViewMut, KeyValuePair, PageFull, PageHeader};
use crate::Result;

pub(crate) trait NodeHeader: Unpin {
    fn left_link(&self) -> Archived<Option<PageIndex>>;

    fn set_left_link(&mut self, left_link: PageIndex);

    fn set_right_link(&mut self, right_link: PageIndex);
}

/// Abstraction over `Leaf` and `Interior` btree nodes
pub(crate) trait NodeView<'a, K, V>: Sized
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    type ArchivedNodeHeader: NodeHeader;

    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, V>>;

    fn page_header(&self) -> &Archived<PageHeader>;

    fn node_header(&self) -> &Self::ArchivedNodeHeader;

    /// A node has a low key iff it is not the root.
    /// All other nodes have a low key. In particular, the non-root left-most nodes have `K::MIN` as the low key.
    // FIXME run into lifetime issues if we try to write a default implementation
    // the impl copied into each implementation for now
    fn low_key(&self) -> Option<&K::Archived>;

    fn is_root(&self) -> bool {
        self.page_header().flags.contains(Flags::IS_ROOT)
    }

    fn len(&self) -> usize {
        self.slotted_page().len()
    }
}

pub(crate) trait NodeMut<K, V>: Sized
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    type ViewMut<'a>: NodeViewMut<'a, K, V>;

    unsafe fn view_mut(data: &mut [u8; PAGE_DATA_SIZE]) -> Result<Self::ViewMut<'_>>;

    /// Initialize a new node with the given flags and data.
    /// This may not assume that the data is zeroed.
    /// Set any additional flags as appropriate, but do not unset any flags.
    fn initialize_with_flags(flags: Flags, data: &mut [u8; PAGE_DATA_SIZE]) -> Self::ViewMut<'_>;

    fn initialize(data: &mut [u8; PAGE_DATA_SIZE]) -> Self::ViewMut<'_> {
        Self::initialize_with_flags(Flags::empty(), data)
    }

    fn initialize_root(data: &mut [u8; PAGE_DATA_SIZE]) -> Self::ViewMut<'_> {
        Self::initialize_with_flags(Flags::IS_ROOT, data)
    }

    /// Split node contents into left and right children and leave the root node empty.
    /// This is intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    fn split_root_into(
        root: &mut Self::ViewMut<'_>,
        left_page_idx: PageIndex,
        left: &mut Self::ViewMut<'_>,
        right_page_idx: PageIndex,
        right: &mut Self::ViewMut<'_>,
    ) {
        assert!(root.is_root());
        assert!(root.slotted_page().len() >= 3);
        assert!(left.slotted_page().is_empty());
        assert!(right.slotted_page().is_empty());

        let slots = root.slotted_page().slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for &slot in lhs {
            let value = root.slotted_page().get_by_slot(slot);
            left.slotted_page_mut().insert(value).unwrap();
        }

        for &slot in rhs {
            let value = root.slotted_page().get_by_slot(slot);
            right.slotted_page_mut().insert(value).unwrap();
        }

        right.set_left_link(left_page_idx);
        left.set_right_link(right_page_idx);

        root.slotted_page_mut().set_len(0);
    }

    fn split_left_into(
        view: &mut Self::ViewMut<'_>,
        view_page_idx: PageIndex,
        left: &mut Self::ViewMut<'_>,
        left_page_idx: PageIndex,
    ) {
        assert!(view.slotted_page().len() >= 3);
        assert!(left.slotted_page().is_empty());

        let slots = view.slotted_page().slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);

        let ours = view.slotted_page_mut();
        let theirs = left.slotted_page_mut();
        for &slot in rhs {
            let value = ours.get_by_slot(slot);
            theirs.insert(value).expect("new page should not be full");
        }

        ours.set_len(lhs.len() as u16);

        view.set_left_link(left_page_idx);
        left.set_right_link(view_page_idx);
    }
}

pub(crate) trait NodeViewMut<'a, K, V>: NodeView<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    fn node_header_mut(&mut self) -> Pin<&mut Self::ArchivedNodeHeader>;

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, KeyValuePair<K, V>>;

    /// SAFETY: The page header must be archived at the start of the page
    /// This is assumed by the implementation of `raw_bytes_mut`
    unsafe fn page_header_mut(&mut self) -> Pin<&mut Archived<PageHeader>>;

    fn raw_bytes_mut(&mut self) -> &mut [u8; PAGE_DATA_SIZE] {
        // page_header is where the start of the raw_bytes so we can just cast the pointer
        unsafe { &mut *(&mut *self.page_header_mut() as *mut _ as *mut [u8; PAGE_DATA_SIZE]) }
        //                   ^ to get the pointer out of the pin
    }

    /// Reinitialize the root node as a root interior node.
    /// This is intended for use when splitting a root node.
    fn reinitialize_as_root_interior(&mut self) -> InteriorPageViewMut<'_, K> {
        assert!(self.page_header().flags.contains(Flags::IS_ROOT));
        InteriorPageViewMut::initialize_root(self.raw_bytes_mut())
    }

    fn set_left_link(&mut self, left_link: PageIndex) {
        self.node_header_mut().set_left_link(left_link);
    }

    fn set_right_link(&mut self, right_link: PageIndex) {
        self.node_header_mut().set_right_link(right_link);
    }

    fn insert(&mut self, key: K::Archived, value: impl Into<V::Archived>) -> Result<(), PageFull> {
        if let Some(low_key) = self.low_key() {
            assert!(low_key <= &key, "key must be no less than low key {low_key:?} !<= {key:?}");
        }

        self.slotted_page_mut().insert(&ArchivedKeyValuePair::new(key, value.into()))
    }
}
