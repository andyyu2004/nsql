use std::marker::PhantomData;
use std::ops::{AddAssign, Deref, Index, IndexMut, Sub, SubAssign};
use std::pin::Pin;
use std::{fmt, mem, ptr, slice};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_rkyv::DefaultSerializer;
use nsql_util::static_assert_eq;
use rkyv::rend::BigEndian;
use rkyv::{Archive, Archived, Deserialize, Infallible, Serialize};

use super::entry::Entry;
use super::{archived_size_of, PageFull};

// NOTE: the layout of this MUST match the layout of the mutable version
#[repr(C)]
#[derive(Copy, Clone)]
pub(crate) struct SlottedPageView<'a, K: Archive, V: Archive> {
    header: &'a Archived<SlottedPageMeta>,
    slots: &'a [Slot],
    data: &'a [u8],
    marker: PhantomData<&'a (K::Archived, V::Archived)>,
}

impl<'a, K, V> fmt::Debug for SlottedPageView<'a, K, V>
where
    K: Archive,
    K::Archived: Ord + fmt::Debug,
    V: Archive,
    V::Archived: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageView")
            .field("header", &self.header)
            .field("values", &self.values().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<'a, K: Archive, V: Archive> SlottedPageView<'a, K, V> {
    /// Safety: `buf` must contain a valid slotted page
    pub(crate) unsafe fn view(buf: &'a [u8]) -> SlottedPageView<'a, K, V> {
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

impl<'a, K, V> SlottedPageView<'a, K, V>
where
    K: Archive,
    K::Archived: Ord,
    V: Archive,
{
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V::Archived>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        let slot = self.slot_of(key)?;
        Some(&self.get_by_slot(slot).value)
    }

    fn values(&self) -> impl Iterator<Item = &V::Archived> {
        self.slots.iter().map(move |&slot| &self.get_by_slot(slot).value)
    }

    fn slot_of<Q>(&self, key: &Q) -> Option<Slot>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        let offset = self.slot_index_of_key(key);
        offset.ok().map(|offset| self.slots[offset])
    }

    pub(crate) fn get_by_slot(&self, slot: Slot) -> &Entry<K::Archived, V::Archived> {
        unsafe { rkyv::archived_root::<Entry<&K, &V>>(&self[slot]) }
    }

    pub(crate) fn low_key(&self) -> Option<&K::Archived> {
        let slot = *self.slots.first()?;
        Some(&self.get_by_slot(slot).key)
    }

    // FIXME cleanup all this api mess
    pub(super) fn slot_index_of_key<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        self.slots.binary_search_by(|slot| {
            let probe = self.get_by_slot(*slot);
            // FIXME introduce another trait for this or something with a blanket impl for `Ord` types
            probe.key.partial_cmp(key).expect("`Q` comparisons must be total with `K`")
        })
    }
}

impl<K: Archive, V: Archive> Index<Slot> for SlottedPageView<'_, K, V> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        // adjust the offset to be relative to the start of the slots
        // as we keep shifting the slots and data around the actual offsets change dependending on the number of slots
        let adjusted_slot_offset: SlotOffset = slot.offset - mem::size_of_val(self.slots) as u16;
        debug_assert_eq!(
            mem::size_of_val(self.slots) as u16,
            self.header.slot_len.value() * archived_size_of!(Slot)
        );
        let adjusted_offset = adjusted_slot_offset.0.value() as usize;
        let end = adjusted_offset + slot.length.value() as usize;
        debug_assert!(end <= PAGE_DATA_SIZE);
        &self.data[adjusted_offset..end]
    }
}

impl<'a, K: Archive + 'static, V: Archive + 'static> Index<Slot> for SlottedPageViewMut<'a, K, V> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        &(**self)[slot]
    }
}

impl<'a, K: Archive + 'static, V: Archive + 'static> IndexMut<Slot>
    for SlottedPageViewMut<'a, K, V>
{
    fn index_mut(&mut self, slot: Slot) -> &mut Self::Output {
        let adjusted_slot_offset: SlotOffset =
            slot.offset - self.header.slot_len * archived_size_of!(Slot);
        let adjusted_offset = adjusted_slot_offset.0.value() as usize;
        &mut self.data[adjusted_offset..adjusted_offset + slot.length.value() as usize]
    }
}

// NOK, VE: must match the layout of `SlottedPageView`
#[derive(Eq)]
#[repr(C)]
pub(crate) struct SlottedPageViewMut<'a, K, V> {
    header: Pin<&'a mut Archived<SlottedPageMeta>>,
    slots: &'a mut [Slot],
    data: &'a mut [u8],
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V> fmt::Debug for SlottedPageViewMut<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageViewMut")
            .field("header", &self.header)
            .field("slots", &self.slots)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V> PartialEq for SlottedPageViewMut<'a, K, V> {
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

impl<'a, K, V> SlottedPageViewMut<'a, K, V> {
    /// `prefix_size` is the size of the page header and the page-specific header` (and anything else that comes before the slots)
    /// slot offsets are relative to the initla value of `free_start`
    pub(crate) fn init(buf: &'a mut [u8], prefix_size: u16) -> SlottedPageViewMut<'a, K, V> {
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
    pub(crate) unsafe fn view_mut(buf: &'a mut [u8]) -> SlottedPageViewMut<'a, K, V> {
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

impl<'a, K: Archive + 'static, V: Archive + 'static> Deref for SlottedPageViewMut<'a, K, V> {
    type Target = SlottedPageView<'a, K, V>;

    fn deref(&self) -> &Self::Target {
        static_assert_eq!(
            mem::size_of::<SlottedPageView<'a, u16, u64>>(),
            mem::size_of::<SlottedPageViewMut<'a, u16, u64>>()
        );

        // SAFETY this is safe because SlottedPageView and SlottedPageViewMut have the same layout
        unsafe { &*(self as *const _ as *const Self::Target) }
    }
}

impl<'a, K, V> SlottedPageViewMut<'a, K, V>
where
    K: Archive + 'static,
    K::Archived: Ord,
    V: Archive + 'static,
{
    /// Safety: `serialized_entry` must be the serialized bytes of an `Entry<&K, &V>`
    pub(crate) unsafe fn insert_raw(
        &mut self,
        serialized_entry: &[u8],
    ) -> Result<Option<V>, PageFull>
    where
        V::Archived: Deserialize<V, Infallible> + fmt::Debug,
    {
        if serialized_entry.len() > PAGE_DATA_SIZE / 4 {
            // we are dividing by 4 not 3 as we're not considering the size of the metadata etc
            todo!(
                "value is too large, we must fit at least 3 items into a page (need to implement overflow pages)"
            );
        }
        // we are dividing by 4 not 3 as we're not considering the size of the metadata etc

        let entry = unsafe { rkyv::archived_root::<Entry<&K, &V>>(serialized_entry) };
        let idx = self.slot_index_of_key(&entry.key);

        let length = serialized_entry.len() as u16;
        let has_space =
            length + archived_size_of!(Slot) <= self.header.free_end - self.header.free_start;

        let prev = match idx {
            Ok(idx) => {
                // key already exists, overwrite the slot to point to the new value
                cov_mark::hit!(slotted_page_insert_duplicate);
                let prev_slot = self.slots[idx];
                let prev = nsql_rkyv::deserialize(unsafe {
                    &rkyv::archived_root::<Entry<&K, &V>>(&self[prev_slot]).value
                });

                let slot = if prev_slot.length >= length {
                    cov_mark::hit!(slotted_page_insert_duplicate_reuse);
                    if !has_space {
                        cov_mark::hit!(slotted_page_insert_duplicate_full_reuse);
                    }
                    // if the previous slot is large enough, we just reuse it
                    // FIXME: need to mark the (prev_slot_length - length) as free space
                    prev_slot
                } else {
                    if !has_space {
                        todo!("page is full with duplicate key, need to do something");
                    }
                    // otherwise, we need to allocate a new slot
                    // FIXME: need to mark the previous space as free space
                    self.header.free_end -= length;
                    self.header.free_start += archived_size_of!(Slot);
                    Slot { offset: self.header.free_end, length: length.into() }
                };

                self[slot].copy_from_slice(serialized_entry);
                debug_assert_eq!(&self[slot], serialized_entry);

                self.slots[idx] = slot;
                Some(prev)
            }
            Err(idx) => {
                if !has_space {
                    return Err(PageFull);
                }

                self.header.free_end -= length;
                self.header.free_start += archived_size_of!(Slot);
                let slot = Slot { offset: self.header.free_end, length: length.into() };
                self[slot].copy_from_slice(serialized_entry);
                debug_assert_eq!(&self[slot], serialized_entry);

                unsafe {
                    // shift everything right of `idx` to the right by 1 (include `idx`) to make space
                    ptr::copy(
                        self.slots.as_ptr().add(idx),
                        self.slots.as_mut_ptr().add(idx + 1),
                        self.slots.len() - idx,
                    );

                    // write the new slot in the hole
                    ptr::write(self.slots.as_mut_ptr().add(idx), slot);

                    // recreate the slice to include the new entry
                    self.header.slot_len += 1;
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
                None
            }
        };

        #[cfg(debug_assertions)]
        self.assert_sorted();

        Ok(prev)
    }

    /// Inserts a value into the page, returning a deserialized copy of the previous value if it already existed
    pub(crate) fn insert(&mut self, key: &K, value: &V) -> Result<Option<V>, PageFull>
    where
        K: Serialize<DefaultSerializer>,
        V: Serialize<DefaultSerializer>,
        V::Archived: Deserialize<V, Infallible> + fmt::Debug,
    {
        let entry = Entry { key, value };
        let bytes = nsql_rkyv::to_bytes(&entry);
        unsafe { self.insert_raw(&bytes) }
    }

    #[cfg(debug_assertions)]
    fn assert_sorted(&self) {
        let mut values = Vec::<&Entry<K::Archived, V::Archived>>::with_capacity(
            self.header.slot_len.value() as usize,
        );
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
