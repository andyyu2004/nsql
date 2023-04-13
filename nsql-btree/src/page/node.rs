use std::fmt;
use std::pin::Pin;

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_rkyv::DefaultSerializer;
use rkyv::option::ArchivedOption;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{Flags, InteriorPageViewMut, PageFull, PageHeader};
use crate::btree::ConcurrentSplit;
use crate::Result;

pub(crate) trait NodeHeader: Unpin {
    fn right_link(&self) -> Option<PageIndex>;

    fn set_left_link(&mut self, left_link: PageIndex);

    fn set_right_link(&mut self, right_link: PageIndex);
}

// FIXME this isn't actually used below
pub(crate) trait Extra<K: Archive> {
    fn high_key(&self) -> &Archived<Option<K>>;

    fn set_high_key(&mut self, high_key: Archived<Option<K>>);
}

/// Abstraction over `Leaf` and `Interior` btree nodes
pub(crate) trait NodeView<'a, K, V>: Sized
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    type ArchivedNodeHeader: NodeHeader;

    type Extra: Archive + 'a;

    fn slotted_page(&self) -> &SlottedPageView<'a, K, V, Self::Extra>;

    fn page_header(&self) -> &Archived<PageHeader>;

    fn node_header(&self) -> &Self::ArchivedNodeHeader;

    /// The "high key" of a node as part of L&Y btrees.
    /// A node has a high key iff it is not the rightmost page.
    /// A `None` high key effectively represents `infinity`.
    fn high_key(&self) -> &Archived<Option<K>>;

    /// The smallest/leftmost key in the node.
    fn min_key(&self) -> Option<&K::Archived>;

    fn ensure_can_contain<Q>(&self, key: &Q) -> Result<(), ConcurrentSplit>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        if !self.can_contain(key) { Err(ConcurrentSplit) } else { Ok(()) }
    }

    fn can_contain<Q>(&self, key: &Q) -> bool
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        match self.high_key().as_ref() {
            Some(high_key) => high_key >= key,
            None => true,
        }
    }

    fn right_link(&self) -> Option<PageIndex> {
        self.node_header().right_link()
    }

    fn is_rightmost(&self) -> bool {
        self.right_link().is_none()
    }

    fn is_root(&self) -> bool {
        self.page_header().flags.contains(Flags::IS_ROOT)
    }

    fn len(&self) -> usize {
        self.slotted_page().len()
    }
}

pub(crate) trait NodeMut<K, V>: Sized
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug + 'static,
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
    ) where
        K::Archived: Clone,
        V::Archived: Deserialize<V, rkyv::Infallible> + fmt::Debug,
    {
        assert!(root.is_root());
        assert!(root.slotted_page().len() >= 3);
        assert!(left.slotted_page().is_empty());
        assert!(right.slotted_page().is_empty());

        let slots = root.slotted_page().slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for &slot in lhs {
            let entry_bytes = &root.slotted_page()[slot];
            assert!(
                unsafe { left.slotted_page_mut().insert_raw(entry_bytes) }
                    .expect("should have sufficient space")
                    .is_none(),
                "should not have a previous value"
            )
        }

        for &slot in rhs {
            let entry_bytes = &root.slotted_page()[slot];
            assert!(
                unsafe { right.slotted_page_mut().insert_raw(entry_bytes) }
                    .expect("should have sufficient space")
                    .is_none(),
                "should not have a previous value"
            )
        }

        left.set_right_link(right_page_idx);
        left.set_high_key(ArchivedOption::Some(
            right.min_key().expect("rhs should be non-empty and therefore have a min key").clone(),
        ));
        right.set_left_link(left_page_idx);

        root.slotted_page_mut().truncate(0);
    }

    /// split `left` into a newly allocated page `right`
    fn split(
        left: &mut Self::ViewMut<'_>,
        left_page_idx: PageIndex,
        right: &mut Self::ViewMut<'_>,
        right_page_idx: PageIndex,
        prev_right_page: Option<&mut Self::ViewMut<'_>>,
    ) where
        K::Archived: Deserialize<K, rkyv::Infallible> + Clone,
        V::Archived: Deserialize<V, rkyv::Infallible>,
    {
        assert!(left.slotted_page().len() >= 3);
        assert!(right.slotted_page().is_empty());

        // update the left link of the former right page to point to the new right page
        if let Some(prev_right_page) = prev_right_page {
            let prev_right_link = left.right_link().expect("should match `prev_right_page`");
            prev_right_page.set_left_link(right_page_idx);
            right.set_right_link(prev_right_link);
        }

        let initial_left_high_key = left.high_key().clone();

        let left_slots = left.slotted_page_mut();
        let slots = left_slots.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);

        let right_slots = right.slotted_page_mut();

        // copy over the right entries to the new right page
        for &slot in rhs {
            let entry_bytes = &left_slots[slot];
            assert!(
                unsafe { right_slots.insert_raw(entry_bytes) }
                    .expect("new page should not be full")
                    .is_none(),
                "should not have a previous value"
            );
        }

        left_slots.truncate(lhs.len() as u16);

        left.set_right_link(right_page_idx);
        left.set_high_key(match right_slots.first() {
            Some(key) => ArchivedOption::Some(key.clone()),
            None => ArchivedOption::None,
        });

        right.set_left_link(left_page_idx);
        right.set_high_key(initial_left_high_key);

        debug_assert_eq!(left.len(), lhs.len());
        debug_assert_eq!(right.len(), rhs.len());
    }
}

pub(crate) trait NodeViewMut<'a, K, V>: NodeView<'a, K, V>
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    fn node_header_mut(&mut self) -> Pin<&mut Self::ArchivedNodeHeader>;

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, K, V, Self::Extra>;

    fn set_high_key(&mut self, high_key: Archived<Option<K>>);

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
    fn reinitialize_as_root_interior(&mut self) -> InteriorPageViewMut<'_, K>
    where
        K: Serialize<DefaultSerializer>,
    {
        assert!(self.page_header().flags.contains(Flags::IS_ROOT));
        let root_interior = InteriorPageViewMut::initialize_root(self.raw_bytes_mut());
        assert!(root_interior.is_root());
        root_interior
    }

    fn set_left_link(&mut self, left_link: PageIndex) {
        self.node_header_mut().set_left_link(left_link);
    }

    fn set_right_link(&mut self, right_link: PageIndex) {
        self.node_header_mut().set_right_link(right_link);
    }

    fn insert(&mut self, key: &K, value: &V) -> Result<Result<Option<V>, PageFull>, ConcurrentSplit>
    where
        K: Serialize<DefaultSerializer>,
        K::Archived: PartialOrd<K>,
        V: Serialize<DefaultSerializer>,
        V::Archived: Deserialize<V, rkyv::Infallible>,
    {
        self.ensure_can_contain(key)?;
        Ok(self.slotted_page_mut().insert(key, value))
    }

    fn remove(&mut self, key: &K) -> Result<Option<V>, ConcurrentSplit>
    where
        K::Archived: PartialOrd<K>,
        V::Archived: Deserialize<V, rkyv::Infallible>,
    {
        self.ensure_can_contain(key)?;
        Ok(self.slotted_page_mut().remove(key))
    }
}
