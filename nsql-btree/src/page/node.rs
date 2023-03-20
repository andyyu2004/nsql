use std::fmt;
use std::pin::Pin;

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use rkyv::{Archive, Archived};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{ArchivedKeyValuePair, Flags, KeyValuePair, PageFull, PageHeader};
use crate::Result;

/// Abstraction over `Leaf` and `Interior` btree nodes
pub(crate) trait Node<'a, K, V>: Sized
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, V>>;

    fn page_header(&self) -> &Archived<PageHeader>;

    fn low_key(&self) -> Option<&K::Archived>;

    fn len(&self) -> usize {
        self.slotted_page().len()
    }
}

pub(crate) trait NodeMut<'a, K, V>: Node<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    /// Initialize a new node with the given flags and data.
    /// This may not assume that the data is zeroed.
    /// Set any additional flags as appropriate, but do not unset any flags.
    fn initialize_with_flags(flags: Flags, data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self;

    unsafe fn view_mut(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Result<Self>;

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, KeyValuePair<K, V>>;

    /// SAFETY: The page header must be archived at the start of the page
    /// This is assumed by the implementation of `raw_bytes_mut`
    unsafe fn page_header_mut(&mut self) -> Pin<&mut Archived<PageHeader>>;

    fn raw_bytes_mut(&mut self) -> &mut [u8; PAGE_DATA_SIZE] {
        // page_header is where the start of the raw_bytes so we can just cast the pointer
        unsafe { &mut *(&mut *self.page_header_mut() as *mut _ as *mut [u8; PAGE_DATA_SIZE]) }
        //                   ^ to get the pointer out of the pin
    }

    fn initialize(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        Self::initialize_with_flags(Flags::empty(), data)
    }

    fn initialize_root(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        Self::initialize_with_flags(Flags::IS_ROOT, data)
    }

    fn insert(&mut self, key: K::Archived, value: impl Into<V::Archived>) -> Result<(), PageFull> {
        if let Some(low_key) = self.low_key() {
            assert!(low_key < &key);
        }

        self.slotted_page_mut().insert(&ArchivedKeyValuePair::new(key, value.into()))
    }

    /// Split node contents into left and right children and leave the root node empty.
    /// This is intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    fn split_root_into(&mut self, left_page_ldx: PageIndex, left: &mut Self, right: &mut Self) {
        assert!(left.slotted_page().is_empty());
        assert!(right.slotted_page().is_empty());
        assert!(self.slotted_page().len() > 1);

        let slots = self.slotted_page().slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for &slot in lhs {
            let value = self.slotted_page().get_by_slot(slot);
            left.slotted_page_mut().insert(value).unwrap();
        }

        for &slot in rhs {
            let value = self.slotted_page().get_by_slot(slot);
            right.slotted_page_mut().insert(value).unwrap();
        }

        self.slotted_page_mut().set_len(0);
    }
}
