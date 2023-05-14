use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Deref, Index, IndexMut, Range, Sub};
use std::pin::Pin;
use std::{fmt, io, mem, slice};

use nsql_pager::{PageIndex, PageReadGuard, PAGE_DATA_SIZE};
use nsql_rkyv::{align_archived_ptr_offset, archived_size_of, DefaultDeserializer};
use rkyv::option::ArchivedOption;
use rkyv::rend::BigEndian;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use super::Versioned;
use crate::Transaction;

const HEAP_PAGE_HEADER_MAGIC: [u8; 4] = *b"HEAP";

#[repr(C)]
pub(crate) struct HeapView<'a, T: Archive> {
    header: &'a Archived<HeapPageHeader>,
    slots: &'a [Archived<Slot>],
    data: &'a [u8],
    marker: PhantomData<&'a T::Archived>,
}

impl<'a, T: Archive> HeapView<'a, T> {
    pub fn view(buf: &'a PageReadGuard<'a>) -> nsql_buffer::Result<Self> {
        let (header, buf) = buf.split_array_ref();
        let header = unsafe { nsql_rkyv::archived_root::<HeapPageHeader>(header) };
        if header.magic != HEAP_PAGE_HEADER_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid heap page magic: {}", unsafe {
                    std::str::from_utf8_unchecked(&header.magic)
                }),
            ))?
        }

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at(slot_len * mem::size_of::<Archived<Slot>>());
        let slots =
            unsafe { slice::from_raw_parts(slot_bytes.as_ptr() as *mut Archived<Slot>, slot_len) };

        Ok(Self { header, slots, data, marker: PhantomData })
    }

    #[inline]
    pub fn right_link(&self) -> Option<PageIndex> {
        self.header.right_link.as_ref().map(|&idx| idx.into())
    }

    #[inline]
    pub fn free_space(&self) -> u16 {
        self.header.free_end.0 - self.header.free_start.0
    }

    #[inline]
    pub fn get_raw(&self, slot: SlotIndex) -> &[u8] {
        &self[slot]
    }

    #[inline]
    pub fn get_rkyv(&self, slot: SlotIndex) -> &Archived<Versioned<'a, T>> {
        unsafe { rkyv::archived_root::<Versioned<'a, T>>(&self[slot]) }
    }

    /// Returns the value in the slot at the given index.
    /// Returns `None` if the transaction cannot see the version of the value.
    #[inline]
    pub fn get(&self, tx: &Transaction, idx: SlotIndex) -> Option<T>
    where
        T::Archived: Deserialize<T, DefaultDeserializer>,
    {
        let raw = self.get_rkyv(idx);
        tx.can_see(raw.version.into()).then(|| nsql_rkyv::deserialize(&raw.data))
    }

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

impl<'a, T: Archive> HeapView<'a, T> {
    #[tracing::instrument(skip(self, acc, f))]
    pub fn scan_into<U>(
        &self,
        tx: &Transaction,
        acc: &mut Vec<U>,
        mut f: impl FnMut(SlotIndex, &T::Archived) -> U,
    ) {
        acc.reserve(self.slots.len());
        self.slots.iter().enumerate().for_each(|(idx, &slot)| {
            let raw = unsafe { rkyv::archived_root::<Versioned<'_, T>>(&self[slot]) };
            if tx.can_see(raw.version.into()) {
                // TODO apply projection to raw tuple data, this view needs to know its page idx to construct the TupleId
                // Treat the column index `n` as the `tid` for now
                acc.push(f(SlotIndex(idx as u16), &raw.data));
            }
        })
    }
}

impl<'a, T: Archive> Index<Slot> for HeapView<'a, T> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        &self.data[self.adjusted_slot_range(slot)]
    }
}

impl<'a, T: Archive + 'a> Index<SlotIndex> for HeapView<'a, T> {
    type Output = [u8];

    fn index(&self, idx: SlotIndex) -> &Self::Output {
        &self[self.slots[idx.0 as usize]]
    }
}

// This layout must remain consistent with the layout of `HeapView` as we will transmute from the
// mutable version to the immutable version.
#[repr(C)]
pub(crate) struct HeapViewMut<'a, T: Archive> {
    header: Pin<&'a mut Archived<HeapPageHeader>>,
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

    fn index(&self, slot: Slot) -> &Self::Output {
        &(**self)[slot]
    }
}

impl<'a, T: Archive> IndexMut<Slot> for HeapViewMut<'a, T> {
    fn index_mut(&mut self, slot: Slot) -> &mut Self::Output {
        let range = self.adjusted_slot_range(slot);
        &mut self.data[range]
    }
}

impl<'a, T: Archive + 'a> Index<SlotIndex> for HeapViewMut<'a, T> {
    type Output = [u8];

    fn index(&self, index: SlotIndex) -> &Self::Output {
        &(**self)[index]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize)]
pub(crate) struct HeapPageHeader {
    magic: [u8; 4],
    free_start: Offset,
    free_end: Offset,
    slot_len: BigEndian<u16>,
    left_link: Option<PageIndex>,
    right_link: Option<PageIndex>,
}

#[derive(Debug)]
pub struct HeapPageFull;

impl<'a, T: Archive> HeapViewMut<'a, T> {
    pub fn initialize(buf: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        let free_end = PAGE_DATA_SIZE as u16 - archived_size_of!(HeapPageHeader);
        let header = HeapPageHeader {
            magic: HEAP_PAGE_HEADER_MAGIC,
            free_start: Offset::from(0),
            free_end: Offset::from(free_end),
            slot_len: 0.into(),
            left_link: None,
            right_link: None,
        };

        let header_bytes = nsql_rkyv::to_bytes(&header);
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);

        Self::view_mut(buf).expect("should have valid magic")
    }

    pub fn view_mut(buf: &'a mut [u8; PAGE_DATA_SIZE]) -> io::Result<Self> {
        let (header, buf) = buf.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<HeapPageHeader>(header) };
        if header.magic != HEAP_PAGE_HEADER_MAGIC {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid heap page magic: {}", unsafe {
                    std::str::from_utf8_unchecked(&header.magic)
                }),
            ))?
        }

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Archived<Slot>>());
        let slots = unsafe {
            slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Archived<Slot>, slot_len)
        };

        Ok(Self { header, slots, data, marker: PhantomData })
    }

    /// Safety: the caller must ensure that the serialized value is a valid archived `Versioned<'a, T>`
    #[inline]
    pub unsafe fn update_in_place_raw(&mut self, idx: SlotIndex, serialized_value: &[u8]) {
        let size = self.slots[idx.0 as usize].length.value();
        let new_size = serialized_value.len() as u16;
        assert!(size >= new_size, "cannot grow a tuple inplace");
        let slot = &mut self.slots[idx.0 as usize];
        slot.length = new_size.into();
        let slot = *slot;
        self[slot].copy_from_slice(serialized_value);
    }

    /// Safety: the caller must ensure that the serialized value is a valid archived `Versioned<'a, T>`
    pub unsafe fn append_raw(
        &mut self,
        _tx: &Transaction,
        serialized_value: &[u8],
    ) -> Result<SlotIndex, HeapPageFull> {
        let length = serialized_value.len() as u16;
        if length > PAGE_DATA_SIZE as u16 / 4 {
            // we are dividing by 4 not 3 as we're not considering the size of the metadata etc
            todo!(
                "value is too large, we must fit at least 3 items into a page (need to implement overflow pages)"
            );
        }

        // initial naive check to see if we have enough space
        // this is necesary to avoid overflow
        if self.header.free_start + archived_size_of!(Slot) + length > self.header.free_end {
            return Err(HeapPageFull);
        }

        let slot = self.new_aligned_slot(self.header.free_end - length, length);
        // recheck accounting for alignment
        if slot.offset < self.header.free_start + archived_size_of!(Slot) {
            return Err(HeapPageFull);
        }

        self.header.free_start += archived_size_of!(Slot);
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

    pub(crate) fn set_left_link(&mut self, link: PageIndex) {
        self.header.left_link = ArchivedOption::Some(link.into());
    }

    pub(crate) fn set_right_link(&mut self, link: PageIndex) {
        self.header.right_link = ArchivedOption::Some(link.into());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive)]
#[archive(as = "Self")]
pub(crate) struct Slot {
    offset: Offset,
    length: BigEndian<u16>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct SlotIndex(u16);

impl From<u16> for SlotIndex {
    fn from(idx: u16) -> Self {
        Self(idx)
    }
}

impl fmt::Display for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize)]
#[archive(as = "Self")]
#[repr(transparent)]
pub(crate) struct Offset(BigEndian<u16>);

impl AddAssign<u16> for Offset {
    fn add_assign(&mut self, rhs: u16) {
        self.0 += rhs;
    }
}

impl Add<u16> for Offset {
    type Output = Self;

    fn add(self, rhs: u16) -> Self::Output {
        Self::from(self.0 + rhs)
    }
}

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
