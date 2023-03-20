use std::fmt;

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use rkyv::{Archive, Archived};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{Flags, PageHeader};
use crate::Result;

/// Abstraction over `Leaf` and `Interior` btree nodes
pub(crate) trait Node<'a, T>
where
    T: Archive,
    T::Archived: Ord + fmt::Debug,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, T>;

    fn page_header(&self) -> &Archived<PageHeader>;
}

pub(crate) trait NodeMut<'a, T>: Node<'a, T> + Sized
where
    T: Archive,
    T::Archived: Ord + fmt::Debug,
{
    /// Initialize a new node with the given flags and data.
    /// This may not assume that the data is zeroed.
    /// Set any additional flags as appropriate, but do not unset any flags.
    fn init_with_flags(flags: Flags, data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self;

    fn init(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        Self::init_with_flags(Flags::empty(), data)
    }

    fn init_root(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        Self::init_with_flags(Flags::IS_ROOT, data)
    }

    unsafe fn view_mut(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Result<Self>;

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, T>;

    /// Split node contents into left and right children and leave the root node empty.
    /// This is intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    fn split_root_into(&mut self, left_page_idx: PageIndex, left: &mut Self, right: &mut Self) {
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
