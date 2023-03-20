use std::marker::PhantomData;
use std::ops::{AddAssign, Deref, Index, IndexMut, Sub, SubAssign};
use std::pin::Pin;
use std::{fmt, mem, ptr, slice};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_util::static_assert_eq;
use rkyv::rend::BigEndian;
use rkyv::{Archive, Archived};

use super::key_value_pair::KeyOrd;
use super::{archived_size_of, PageFull};
use crate::page::KeyValuePair;

// NOTE: the layout of this MUST match the layout of the mutable version
#[repr(C)]
#[derive(Copy, Clone)]
pub(crate) struct SlottedPageView<'a, T> {
    header: &'a Archived<SlottedPageMeta>,
    slots: &'a [Slot],
    data: &'a [u8],
    marker: PhantomData<T>,
}

impl<'a, T> fmt::Debug for SlottedPageView<'a, T>
where
    T: Archive,
    T::Archived: Ord + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageView")
            .field("header", &self.header)
            .field("values", &self.values().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<'a, T> SlottedPageView<'a, T> {
    /// Safety: `buf` must contain a valid slotted page
    pub(crate) unsafe fn view(buf: &'a [u8]) -> SlottedPageView<'a, T> {
        let (header_bytes, buf) = buf.split_array_ref();
        let header = unsafe { nsql_rkyv::archived_root::<SlottedPageMeta>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at(slot_len * mem::size_of::<Slot>());

        let slots = unsafe { slice::from_raw_parts(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Self { header, slots, data, marker: PhantomData }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn slots(&self) -> &'a [Slot] {
        self.slots
    }
}

impl<'a, T> SlottedPageView<'a, T>
where
    T: Archive,
    T::Archived: Ord + fmt::Debug,
{
    pub(crate) fn get<K>(&self, key: &K) -> Option<&T::Archived>
    where
        T::Archived: KeyOrd<Key = K>,
    {
        let slot = self.slot_of(key)?;
        Some(self.get_by_slot(slot))
    }

    fn values(&self) -> impl Iterator<Item = &T::Archived> {
        self.slots.iter().map(move |&slot| self.get_by_slot(slot))
    }

    fn slot_of<K>(&self, key: &K) -> Option<Slot>
    where
        K: ?Sized,
        T::Archived: KeyOrd<Key = K>,
    {
        let offset = self.slot_index_of_key(key);
        offset.ok().map(|offset| self.slots[offset])
    }

    pub(crate) fn get_by_slot(&self, slot: Slot) -> &T::Archived {
        unsafe { rkyv::archived_root::<T>(&self[slot]) }
    }

    pub(crate) fn low_key(&self) -> &T::Archived {
        let slot = *self.slots.first().unwrap();
        self.get_by_slot(slot)
    }

    // FIXME cleanup all this api mess
    pub(super) fn slot_index_of_key<K>(&self, key: &K) -> Result<usize, usize>
    where
        K: ?Sized,
        T::Archived: KeyOrd<Key = K>,
    {
        self.slots.binary_search_by(|slot| {
            let value = self.get_by_slot(*slot);
            value.key_cmp(key)
        })
    }

    pub(super) fn slot_index_of_value(&self, value: &T::Archived) -> Result<usize, usize> {
        self.slots.binary_search_by(|slot| {
            let v = self.get_by_slot(*slot);
            v.cmp(value)
        })
    }
}

impl<T> Index<Slot> for SlottedPageView<'_, T> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        // adjust the offset to be relative to the start of the slots
        // as we keep shifting the slots and data around the actual offsets change dependending on the number of slots
        let adjusted_slot_offset: SlotOffset =
            slot.offset - self.header.slot_len * archived_size_of!(Slot);
        let adjusted_offset = adjusted_slot_offset.0.value() as usize;
        &self.data[adjusted_offset..adjusted_offset + slot.length.value() as usize]
    }
}

impl<T> Index<Slot> for SlottedPageViewMut<'_, T> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        &(**self)[slot]
    }
}

impl<T> IndexMut<Slot> for SlottedPageViewMut<'_, T> {
    fn index_mut(&mut self, slot: Slot) -> &mut Self::Output {
        let adjusted_slot_offset: SlotOffset =
            slot.offset - self.header.slot_len * archived_size_of!(Slot);
        let adjusted_offset = adjusted_slot_offset.0.value() as usize;
        &mut self.data[adjusted_offset..adjusted_offset + slot.length.value() as usize]
    }
}

// NOTE: must match the layout of `SlottedPageView`
#[derive(Eq)]
#[repr(C)]
pub(crate) struct SlottedPageViewMut<'a, T> {
    header: Pin<&'a mut Archived<SlottedPageMeta>>,
    slots: &'a mut [Slot],
    data: &'a mut [u8],
    marker: PhantomData<T>,
}

impl<'a, T> fmt::Debug for SlottedPageViewMut<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageViewMut")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl<'a, T> PartialEq for SlottedPageViewMut<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.slots == other.slots && self.data == other.data
    }
}

#[derive(Debug, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
#[archive(compare(PartialEq))]
struct SlottedPageMeta {
    // start offset initially starts from 0 and end offset is relative to that
    free_start: SlotOffset,
    free_end: SlotOffset,
    slot_len: u16,
}

nsql_util::static_assert_eq!(
    mem::size_of::<SlottedPageMeta>(),
    mem::size_of::<ArchivedSlottedPageMeta>()
);

impl<'a, T> SlottedPageViewMut<'a, T> {
    /// `prefix_size` is the size of the page header and the page-specific header` (and anything else that comes before the slots)
    /// slot offsets are relative to the initla value of `free_start`
    pub(crate) fn init(buf: &'a mut [u8], prefix_size: u16) -> SlottedPageViewMut<'a, T> {
        let free_end = PAGE_DATA_SIZE as u16 - prefix_size - archived_size_of!(SlottedPageMeta);
        assert_eq!(free_end, buf.len() as u16 - archived_size_of!(SlottedPageMeta));
        let header = SlottedPageMeta {
            free_start: SlotOffset::from(0),
            free_end: SlotOffset::from(free_end),
            slot_len: 0,
        };

        let bytes = nsql_rkyv::to_bytes(&header);
        buf[..bytes.len()].copy_from_slice(&bytes);

        unsafe { Self::view_mut(buf) }
    }

    /// Safety: `buf` must point at the start of a valid slotted page
    pub(crate) unsafe fn view_mut(buf: &'a mut [u8]) -> SlottedPageViewMut<'a, T> {
        let (header_bytes, buf) = buf.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<SlottedPageMeta>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Slot>());

        let slots =
            unsafe { slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Slot, slot_len) };

        Self { header, slots, data, marker: PhantomData }
    }

    pub(crate) fn set_len(&mut self, len: u16) {
        self.header.slot_len = len.into();
    }
}

impl<'a, T> Deref for SlottedPageViewMut<'a, T> {
    type Target = SlottedPageView<'a, T>;

    fn deref(&self) -> &Self::Target {
        static_assert_eq!(
            mem::size_of::<SlottedPageView<'a, KeyValuePair<u16, u64>>>(),
            mem::size_of::<SlottedPageViewMut<'a, KeyValuePair<u16, u64>>>()
        );

        // SAFETY this is safe because SlottedPageView and SlottedPageViewMut have the same layout
        unsafe { &*(self as *const _ as *const Self::Target) }
    }
}

impl<'a, T> SlottedPageViewMut<'a, T>
where
    T: Archive,
    T::Archived: Ord + fmt::Debug,
{
    pub(crate) fn insert(&mut self, value: &T::Archived) -> Result<(), PageFull> {
        let serialized_value = unsafe {
            slice::from_raw_parts(value as *const _ as *const u8, mem::size_of_val(value))
        };

        debug_assert_eq!(unsafe { rkyv::archived_root::<T>(serialized_value) }, value);

        let length = serialized_value.len() as u16;
        if length + archived_size_of!(Slot) > self.header.free_end - self.header.free_start {
            return Err(PageFull);
        }

        let idx = self.slot_index_of_value(value);

        let idx = match idx {
            Ok(idx) => todo!("handle case where key already exists {:?}", self.slots[idx]),
            Err(idx) => idx,
        };

        self.header.free_end -= length;
        self.header.free_start += archived_size_of!(Slot);
        let slot = Slot { offset: self.header.free_end, length: length.into() };
        self[slot].copy_from_slice(serialized_value);
        debug_assert_eq!(&self[slot], serialized_value);
        debug_assert_eq!(unsafe { rkyv::archived_root::<T>(&self[slot]) }, value);

        // following line MUST be done after the write to `self[slot]` as it will shift the index operation
        self.header.slot_len += 1;

        unsafe {
            // write the new slot at index `idx` of the slot array

            // shift everything right of `idx` to the right by 1 (inclusive)
            ptr::copy(
                self.slots.as_ptr().add(idx),
                self.slots.as_mut_ptr().add(idx + 1),
                self.slots.len() - idx,
            );

            // write the new slot in the hole
            ptr::write(self.slots.as_mut_ptr().add(idx), slot);

            // recreate the slice to include the new entry
            self.slots = slice::from_raw_parts_mut(
                self.slots.as_mut_ptr(),
                self.header.slot_len.value() as usize,
            );
        }

        // we have to shift over the slice of data as we expect it to start after the slots (at `free_start`)
        // we some small tricks to avoid lifetime issues without using unsafe
        // see https://stackoverflow.com/questions/61223234/can-i-reassign-a-mutable-slice-reference-to-a-sub-slice-of-itself
        let data: &'a mut [u8] = mem::take(&mut self.data);
        let (_, data) = data.split_array_mut::<{ mem::size_of::<Archived<Slot>>() }>();
        self.data = data;

        debug_assert_eq!(unsafe { rkyv::archived_root::<T>(&self[slot]) }, value);

        #[cfg(debug_assertions)]
        self.assert_sorted();

        Ok(())
    }

    #[cfg(debug_assertions)]
    fn assert_sorted(&self) {
        let mut values = Vec::<&T::Archived>::with_capacity(self.header.slot_len.value() as usize);
        for &slot in self.slots.iter() {
            values.push(self.get_by_slot(slot));
        }

        assert!(values.is_sorted());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive)]
#[archive_attr(derive(Debug))]
pub struct Slot {
    /// The offset of the entry from the start of the page
    offset: SlotOffset,
    /// The length of the entry
    // FIXME this is a large use of space for small entries
    // is there a more efficient way to store the length (or an alternative to storing the length entirely)?
    // FIXME use a u8 and multiple the value by some constant to get the proper offset
    length: BigEndian<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, rkyv::Serialize)]
#[archive(as = "Self")]
#[repr(transparent)]
pub struct SlotOffset(BigEndian<u16>);

impl AddAssign<u16> for SlotOffset {
    fn add_assign(&mut self, rhs: u16) {
        self.0 += rhs;
    }
}

impl SubAssign<u16> for SlotOffset {
    fn sub_assign(&mut self, rhs: u16) {
        self.0 -= rhs;
    }
}

impl From<u16> for SlotOffset {
    fn from(offset: u16) -> Self {
        assert!(offset < PAGE_DATA_SIZE as u16);
        Self(BigEndian::from(offset))
    }
}

impl Sub for SlotOffset {
    type Output = u16;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

impl Sub<u16> for SlotOffset {
    type Output = SlotOffset;

    fn sub(self, rhs: u16) -> Self::Output {
        Self::from(self.0 - rhs)
    }
}
