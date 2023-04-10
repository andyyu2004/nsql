use std::marker::PhantomData;
use std::ops::{Deref, Index, IndexMut, Range, Sub};
use std::pin::Pin;
use std::{mem, slice};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_rkyv::{align_archived_ptr_offset, archived_size_of, DefaultSerializer};
use rkyv::rend::BigEndian;
use rkyv::{Archive, Archived, Serialize};

#[repr(C)]
pub(crate) struct HeapView<'a, T: Archive> {
    header: &'a SlottedPageHeader,
    slots: &'a [Archived<Slot>],
    data: &'a [u8],
    marker: PhantomData<&'a T::Archived>,
}

impl<'a, T: Archive> HeapView<'a, T> {
    fn adjusted_slot_range(&self, slot: Slot) -> Range<usize> {
        // adjust the offset to be relative to the start of the slots
        // as we keep shifting the slots and data around the actual offsets change dependending on the number of slots
        let adjusted_slot_offset: Offset = slot.offset - mem::size_of_val(self.slots) as u16;
        debug_assert_eq!(
            mem::size_of_val(self.slots) as u16,
            self.header.slot_len.value() * archived_size_of!(Slot)
        );
        let adjusted_offset = adjusted_slot_offset.0.value() as usize;
        adjusted_offset..adjusted_offset + slot.length.value() as usize
    }
}

impl<'a, T: Archive> Index<Slot> for HeapView<'a, T> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        &self.data[self.adjusted_slot_range(slot)]
    }
}

// This layout must remain consistent with the layout of `HeapView` as we will transmute from the
// mutable version to the immutable version.
#[repr(C)]
pub(crate) struct HeapViewMut<'a, T: Archive> {
    header: Pin<&'a mut SlottedPageHeader>,
    slots: &'a mut [Archived<Slot>],
    data: &'a mut [u8],
    marker: PhantomData<&'a mut T::Archived>,
}

impl<'a, T: Archive> Deref for HeapViewMut<'a, T> {
    type Target = HeapView<'a, T>;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self as *const Self as *const Self::Target) }
    }
}

impl<'a, T: Archive> Index<Slot> for HeapViewMut<'a, T> {
    type Output = [u8];

    fn index(&self, index: Slot) -> &Self::Output {
        &(**self)[index]
    }
}

impl<'a, T: Archive> IndexMut<Slot> for HeapViewMut<'a, T> {
    fn index_mut(&mut self, index: Slot) -> &mut Self::Output {
        let range = self.adjusted_slot_range(index);
        &mut self.data[range]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize)]
#[archive(as = "Self")]
pub(crate) struct SlottedPageHeader {
    free_start: Offset,
    free_end: Offset,
    slot_len: BigEndian<u16>,
}

#[derive(Debug)]
pub struct HeapPageFull;

impl<'a, T: Archive> HeapViewMut<'a, T> {
    pub fn init(buf: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        let free_end = PAGE_DATA_SIZE as u16 - archived_size_of!(SlottedPageHeader);
        let header = SlottedPageHeader {
            free_start: Offset::from(0),
            free_end: Offset::from(free_end),
            slot_len: 0.into(),
        };

        let header_bytes = nsql_rkyv::to_bytes(&header);
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);

        Self::view_mut(buf)
    }

    pub fn view_mut(buf: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        let (header, buf) = buf.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<SlottedPageHeader>(header) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Archived<Slot>>());
        let slots = unsafe {
            slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Archived<Slot>, slot_len)
        };

        Self { header, slots, data, marker: PhantomData }
    }

    pub fn push(&mut self, value: &T) -> Result<SlotIndex, HeapPageFull>
    where
        T: Serialize<DefaultSerializer>,
    {
        let serialized_value = nsql_rkyv::to_bytes(value);
        unsafe { self.push_raw(&serialized_value) }
    }

    pub unsafe fn push_raw(&mut self, serialized_value: &[u8]) -> Result<SlotIndex, HeapPageFull> {
        let length = serialized_value.len() as u16;
        if length > PAGE_DATA_SIZE as u16 / 4 {
            // we are dividing by 4 not 3 as we're not considering the size of the metadata etc
            todo!(
                "value is too large, we must fit at least 3 items into a page (need to implement overflow pages)"
            );
        }

        let slot = self.new_aligned_slot(self.header.free_end - length, length);
        if slot.offset - archived_size_of!(Slot) < self.header.free_start {
            return Err(HeapPageFull);
        }

        self.header.free_end = slot.offset;
        self[slot].copy_from_slice(serialized_value);
        debug_assert_eq!(&self[slot], serialized_value);

        // we have to shift over the slice of data as we expect it to start after the slots (at `free_start`)
        // we some small tricks to avoid lifetime issues without using unsafe
        // see https://stackoverflow.com/questions/61223234/can-i-reassign-a-mutable-slice-reference-to-a-sub-slice-of-itself
        // do this before recreating the slice to avoid potential UB with overlapping mutable references
        (_, self.data) =
            mem::take(&mut self.data).split_array_mut::<{ mem::size_of::<Archived<Slot>>() }>();

        let idx = self.header.slot_len.value() as usize;
        self.slots.as_mut_ptr().add(idx).write(slot);
        self.header.slot_len += 1;
        self.slots = slice::from_raw_parts_mut(
            self.slots.as_mut_ptr(),
            self.header.slot_len.value() as usize,
        );

        Ok(SlotIndex(idx as u16))
    }

    /// Create a new slot with the given offset and length, ensuring that the slot is aligned
    /// by shifting the offset left if necessary>
    fn new_aligned_slot(&self, start_offset: Offset, length: u16) -> Slot {
        Slot { offset: self.aligned_offset(start_offset), length: length.into() }
    }

    fn aligned_offset(&self, offset: Offset) -> Offset {
        let ptr = self[Slot { offset, length: 0.into() }].as_ptr();
        let adjustment = align_archived_ptr_offset::<T>(ptr);
        offset - adjustment as u16
    }

    fn free_space(&self) -> u16 {
        self.header.free_end.0.value() - self.header.free_start.0.value()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive)]
#[archive(as = "Self")]
pub(crate) struct Slot {
    offset: Offset,
    length: BigEndian<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct SlotIndex(u16);

impl From<u16> for SlotIndex {
    fn from(idx: u16) -> Self {
        Self(idx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize)]
#[archive(as = "Self")]
#[repr(transparent)]
pub(crate) struct Offset(BigEndian<u16>);

impl Sub<usize> for Offset {
    type Output = Self;

    fn sub(self, rhs: usize) -> Self::Output {
        self - rhs as u16
    }
}

impl Sub<u16> for Offset {
    type Output = Self;

    fn sub(self, rhs: u16) -> Self::Output {
        Self::from(self.0 - rhs)
    }
}

impl From<u16> for Offset {
    fn from(offset: u16) -> Self {
        Self(BigEndian::from(offset))
    }
}
