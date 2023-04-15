use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Deref, Index, IndexMut, Range, Sub, SubAssign};
use std::pin::Pin;
use std::{fmt, mem, ptr, slice};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_rkyv::{
    align_archived_ptr_offset, archived_align_of, archived_size_of, DefaultSerializer,
};
use nsql_util::static_assert_eq;
use rkyv::rend::BigEndian;
use rkyv::{Archive, Archived, Deserialize, Infallible, Serialize};

use super::entry::Entry;
use super::PageFull;

// NOTE: the layout of this MUST match the layout of the mutable version
#[repr(C)]
#[derive(Copy, Clone)]
pub(crate) struct SlottedPageView<'a, K: Archive, V: Archive, X: Archive = ()> {
    header: &'a Archived<SlottedPageHeader>,
    extra: &'a Archived<X>,
    slots: &'a [Archived<Slot>],
    data: &'a [u8],
    marker: PhantomData<&'a (K::Archived, V::Archived)>,
}

impl<'a, K, V, X> fmt::Debug for SlottedPageView<'a, K, V, X>
where
    K: Archive,
    K::Archived: Ord + fmt::Debug,
    V: Archive,
    V::Archived: fmt::Debug,
    X: Archive,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlottedPageView")
            .field("header", &self.header)
            .field("values", &self.values().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<'a, K: Archive, V: Archive, X: Archive> SlottedPageView<'a, K, V, X> {
    /// Safety: `buf` must contain a valid slotted page
    // Should be analogous to `Self::view_mut`
    pub(crate) unsafe fn view(buf: &'a [u8]) -> SlottedPageView<'a, K, V, X> {
        let (header_bytes, buf) = buf.split_array_ref();
        let header = unsafe { nsql_rkyv::archived_root::<SlottedPageHeader>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at(slot_len * mem::size_of::<Slot>());
        let slots =
            unsafe { slice::from_raw_parts(slot_bytes.as_ptr() as *mut Archived<Slot>, slot_len) };

        let extra_start = data.len() - header.extra_len.value() as usize;
        let (data, extra_bytes) = data.split_at(extra_start);
        let extra = rkyv::archived_root::<X>(extra_bytes);

        Self { header, slots, data, extra, marker: PhantomData }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    fn free_space(&self) -> u16 {
        self.header.free_end.0.value() - self.header.free_start.0.value()
    }

    pub(crate) fn slots(&self) -> &'a [Archived<Slot>] {
        self.slots
    }

    pub(crate) fn extra(&self) -> &'a Archived<X> {
        self.extra
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
        adjusted_offset..adjusted_offset + slot.flags.length() as usize
    }
}

impl<'a, K, V, X> SlottedPageView<'a, K, V, X>
where
    K: Archive,
    K::Archived: Ord,
    V: Archive,
    X: Archive,
{
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V::Archived>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        let slot = self.slot_of(key)?;
        Some(&self.get_by_slot(slot).value)
    }

    pub(crate) fn find_min<Q>(&self, lower_bound: &Q) -> Option<&V::Archived>
    where
        K::Archived: PartialOrd<Q> + fmt::Debug,
        V::Archived: fmt::Debug,
        Q: ?Sized,
    {
        let idx = match self.slot_index_of_key(lower_bound) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        (idx < self.slots.len()).then(|| &self.get_by_slot(self.slots[idx]).value)
    }

    fn values(&self) -> impl Iterator<Item = &V::Archived> + fmt::Debug {
        self.slots.iter().map(move |&slot| &self.get_by_slot(slot).value)
    }

    fn slot_of<Q>(&self, key: &Q) -> Option<Slot>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
    {
        let idx = self.slot_index_of_key(key);
        idx.ok().map(|idx| self.slots[idx])
    }

    pub(crate) fn get_by_slot(&self, slot: Slot) -> &Entry<K::Archived, V::Archived> {
        unsafe { rkyv::archived_root::<Entry<&K, &V>>(&self[slot]) }
    }

    pub(crate) fn first(&self) -> Option<&K::Archived> {
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

impl<K: Archive, V: Archive, X: Archive> Index<Slot> for SlottedPageView<'_, K, V, X> {
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        let range = self.adjusted_slot_range(slot);
        &self.data[range]
    }
}

impl<'a, K: Archive + 'static, V: Archive + 'static, X: Archive> Index<Slot>
    for SlottedPageViewMut<'a, K, V, X>
{
    type Output = [u8];

    fn index(&self, slot: Slot) -> &Self::Output {
        &(**self)[slot]
    }
}

impl<'a, K: Archive + 'static, V: Archive + 'static, X: Archive> IndexMut<Slot>
    for SlottedPageViewMut<'a, K, V, X>
{
    fn index_mut(&mut self, slot: Slot) -> &mut Self::Output {
        let range = self.adjusted_slot_range(slot);
        &mut self.data[range]
    }
}

// NOK, VE: must match the layout of `SlottedPageView`
// #[derive(Eq)]
#[repr(C)]
pub(crate) struct SlottedPageViewMut<'a, K, V, X: Archive = ()> {
    header: Pin<&'a mut Archived<SlottedPageHeader>>,
    extra: Pin<&'a mut Archived<X>>,
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

#[derive(Debug, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
#[archive(compare(PartialEq))]
struct SlottedPageHeader {
    // free_start offset initially starts from 0 and end offset is relative to that
    free_start: Offset,
    free_end: Offset,
    slot_len: u16,
    /// `extra` is stored at the very end of the page
    extra_len: u16,
}

nsql_util::static_assert_eq!(
    mem::size_of::<SlottedPageHeader>(),
    mem::size_of::<ArchivedSlottedPageHeader>()
);

impl<'a, K: Archive, V: Archive, X: Serialize<nsql_rkyv::DefaultSerializer>>
    SlottedPageViewMut<'a, K, V, X>
{
    /// slot offsets are relative to the initial value of `free_start`
    pub(crate) fn init(buf: &'a mut [u8], extra: X) -> SlottedPageViewMut<'a, K, V, X> {
        let extra_bytes = nsql_rkyv::to_bytes(&extra);

        let mut extra_start = buf.len() - extra_bytes.len();
        let offset = align_archived_ptr_offset::<X>(buf[extra_start..].as_ptr());
        extra_start -= offset;
        buf[extra_start..].copy_from_slice(&extra_bytes[..]);

        let free_end = extra_start as u16 - archived_size_of!(SlottedPageHeader);

        let header = SlottedPageHeader {
            free_start: Offset::from(0u16),
            free_end: Offset::from(free_end),
            slot_len: 0,
            extra_len: extra_bytes.len() as u16,
        };

        let bytes = nsql_rkyv::to_bytes(&header);
        buf[..bytes.len()].copy_from_slice(&bytes);

        unsafe { Self::view_mut(buf) }
    }

    /// Safety: `buf` must point at the start of a valid slotted page
    // Should be analogous to `Self::view`
    pub(crate) unsafe fn view_mut(buf: &'a mut [u8]) -> SlottedPageViewMut<'a, K, V, X> {
        let (header_bytes, buf) = buf.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<SlottedPageHeader>(header_bytes) };

        let slot_len = header.slot_len.value() as usize;
        let (slot_bytes, data) = buf.split_at_mut(slot_len * mem::size_of::<Archived<Slot>>());
        let slots = unsafe {
            slice::from_raw_parts_mut(slot_bytes.as_ptr() as *mut Archived<Slot>, slot_len)
        };

        let extra_start = data.len() - header.extra_len.value() as usize;
        let (data, extra_bytes) = data.split_at_mut(extra_start);
        let extra = rkyv::archived_root_mut::<X>(Pin::new(extra_bytes));

        Self { header, slots, data, extra, marker: PhantomData }
    }
}

impl<'a, K: Archive + 'static, V: Archive + 'static, X: Archive> Deref
    for SlottedPageViewMut<'a, K, V, X>
{
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

impl<'a, K, V, X> SlottedPageViewMut<'a, K, V, X>
where
    K: Archive + 'static,
    K::Archived: Ord + fmt::Debug,
    V: Archive + 'static,
    V::Archived: fmt::Debug,
    X: Archive,
{
    /// Truncate the page to the leftmost `new_len` slots
    pub(crate) fn truncate(&mut self, new_len: u16) {
        // FIXME refactor the mess
        // A lot of the bit fiddling stuff is duplicated from `SlottedPageView::insert`

        let prev_data_len = self.data.len();
        assert!(new_len as usize <= prev_data_len);
        // FIXME keep track of freed space for reuse
        self.header.slot_len = new_len.into();

        // shift slots and data slices appropriately
        let (slots, freed_data) = mem::take(&mut self.slots).split_at_mut(new_len as usize);
        self.slots = slots;

        let freed_ptr = freed_data.as_ptr() as *mut u8;
        let freed_bytes = freed_data.len() * mem::size_of::<Slot>();
        assert_eq!(self.data.as_ptr(), freed_ptr.wrapping_add(freed_bytes) as *mut u8);
        self.data = unsafe {
            slice::from_raw_parts_mut(freed_data.as_ptr() as *mut u8, prev_data_len + freed_bytes)
        };
        self.header.free_start -= freed_bytes as u16;

        #[cfg(debug_assertions)]
        self.assert_invariants();
    }

    /// Safety: `serialized_entry` must be the serialized bytes of an `Entry<&K, &V>`
    pub(crate) unsafe fn insert_raw(
        &mut self,
        serialized_entry: &[u8],
    ) -> Result<Option<V>, PageFull>
    where
        V::Archived: Deserialize<V, Infallible> + fmt::Debug,
    {
        match self.insert_raw_inner(serialized_entry) {
            Ok(prev) => Ok(prev),
            Err(PageFull) => {
                self.defragment();
                self.insert_raw_inner(serialized_entry)
            }
        }
    }

    /// Defragment the slotted page by rewriting all the data in slot order with no gaps
    fn defragment(&mut self) {
        tracing::debug!("defragmenting page");
        let mut new_data = Vec::with_capacity(self.data.len());
        // copy the data in slot order into a new buffer
        for &slot in self.slots.iter() {
            let bytes = &self[slot];
            debug_assert_eq!(bytes.len(), slot.flags.length() as usize);
            new_data.extend_from_slice(bytes);
        }

        // The `page_end_offset` is the offset to the very end of the page.
        // We compute it by getting the offset where `data` (i.e. `free_start`) starts and adding its length.
        let page_end_offset: Offset = self.header.free_start + self.data.len() as u16;

        // the new `free_end` is the end of the page minus the size of the compacted data
        self.header.free_end = self.aligned_offset(page_end_offset - new_data.len() as u16);
        assert!(self.header.free_end >= self.header.free_start);

        // update the offsets of the slots in place
        let mut offset = self.header.free_end;
        for slot in self.slots.iter_mut() {
            slot.offset = offset;
            offset += slot.flags.length();
        }

        // copy the buffer back into the page in the appropriate place
        assert!(
            new_data.len() <= self.data.len(),
            "defragmented data should be no larger than original"
        );

        let length = new_data.len() as u16;
        // not really a slot, but we can reuse the indexing logic to copy the data
        let fake_slot = Slot { offset: self.header.free_end, flags: SlotFlags::new(length) };
        self[fake_slot].copy_from_slice(&new_data);

        #[cfg(debug_assertions)]
        self.assert_invariants();
    }

    /// Create a new slot with the given offset and length, ensuring that the slot is aligned
    /// by shifting the offset left if necessary>
    fn new_aligned_slot(&self, offset: Offset, length: u16) -> Slot {
        Slot { offset: self.aligned_offset(offset), flags: SlotFlags::new(length) }
    }

    fn aligned_offset(&self, offset: Offset) -> Offset {
        let ptr = self[Slot { offset, flags: SlotFlags::new(0) }].as_ptr();
        let adjustment = align_archived_ptr_offset::<Entry<&K, &V>>(ptr);
        offset - adjustment as u16
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized,
        V::Archived: Deserialize<V, Infallible> + fmt::Debug,
    {
        let idx = self.slot_index_of_key(key).ok()?;
        let slot = self.slots[idx];
        let prev = nsql_rkyv::deserialize(unsafe {
            &rkyv::archived_root::<Entry<&K, &V>>(&self[slot]).value
        });

        // remove the slot at `idx` by shifting all the slots after it to the left
        self.slots.copy_within(idx + 1.., idx);

        // reshuffle `slots` and `data` (opposite of what is done in `insert`)
        self.header.slot_len -= 1;
        let ptr;
        (self.slots, ptr) =
            mem::take(&mut self.slots).split_at_mut(self.header.slot_len.value() as usize);
        self.header.free_start -= archived_size_of!(Slot);
        let new_data_len = self.data.len() + archived_size_of!(Slot) as usize;
        self.data = unsafe { slice::from_raw_parts_mut(ptr.as_ptr() as *mut u8, new_data_len) };
        Some(prev)
    }

    unsafe fn insert_raw_inner(&mut self, serialized_entry: &[u8]) -> Result<Option<V>, PageFull>
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

        // naive check to see if we have enough space to insert the new entry
        // this does not account for the fact we may have to use extra space due to alignment
        // This may have false positives, but no false negatives.
        let naive_has_space = length + archived_size_of!(Slot) <= self.free_space();

        let prev = match idx {
            Ok(idx) => {
                // key already exists, overwrite the slot to point to the new value
                cov_mark::hit!(slotted_page_insert_duplicate);
                let prev_slot = self.slots[idx];

                let prev = nsql_rkyv::deserialize(unsafe {
                    &rkyv::archived_root::<Entry<&K, &V>>(&self[prev_slot]).value
                });

                let slot = if prev_slot.flags.length() >= length {
                    cov_mark::hit!(slotted_page_insert_duplicate_reuse);
                    if !naive_has_space {
                        cov_mark::hit!(slotted_page_insert_duplicate_full_reuse);
                    }
                    // if the previous slot is large enough, we just reuse it
                    // FIXME: need to mark the (prev_slot_length - length) as free space
                    prev_slot
                } else {
                    if !naive_has_space {
                        todo!("page is full with duplicate key, need to do something");
                    }
                    // otherwise, we need to allocate fresh space
                    // FIXME: need to mark the previous space as free space
                    self.header.free_end -= length;
                    self.header.free_start += archived_size_of!(Slot);
                    Slot { offset: self.header.free_end, flags: SlotFlags::new(length) }
                };

                self[slot].copy_from_slice(serialized_entry);
                debug_assert_eq!(&self[slot], serialized_entry);

                self.slots[idx] = slot;
                Some(prev)
            }
            Err(idx) => {
                if !naive_has_space {
                    return Err(PageFull);
                }

                let slot = self.new_aligned_slot(self.header.free_end - length, length);

                // Recheck whether we actually have free space after aligning the slot.
                // This check will need to be smarter if we start reusing freed space
                let has_space = slot.offset >= self.header.free_start + archived_size_of!(Slot);
                if !has_space {
                    return Err(PageFull);
                }

                self.header.free_end = slot.offset;
                self.header.free_start += archived_size_of!(Slot);
                assert!(
                    self[slot].as_ptr().is_aligned_to(archived_align_of!(Entry<&K, &V>)),
                    "allocated slot is not aligned to the archived entry (align_offset: {})",
                    self[slot].as_ptr().align_offset(archived_align_of!(Entry<&K, &V>)),
                );
                self[slot].copy_from_slice(serialized_entry);
                debug_assert_eq!(&self[slot], serialized_entry);

                // we have to shift over the slice of data as we expect it to start after the slots (at `free_start`)
                // we some small tricks to avoid lifetime issues without using unsafe
                // see https://stackoverflow.com/questions/61223234/can-i-reassign-a-mutable-slice-reference-to-a-sub-slice-of-itself
                // do this before recreating the slice to avoid potential UB with overlapping mutable references
                (_, self.data) = mem::take(&mut self.data)
                    .split_array_mut::<{ mem::size_of::<Archived<Slot>>() }>();

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

                None
            }
        };

        #[cfg(debug_assertions)]
        self.assert_invariants();

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

    pub(crate) fn extra_mut(&mut self) -> Pin<&mut Archived<X>> {
        self.extra.as_mut()
    }

    #[cfg(debug_assertions)]
    fn assert_invariants(&self) {
        assert!(self.header.free_start <= self.header.free_end);
        assert_eq!(self.header.slot_len, self.slots.len() as u16);
        assert_eq!(
            self.slots.as_ptr().wrapping_add(self.slots.len()).cast::<u8>(),
            self.data.as_ptr(),
            "slots and data pointers are not contiguous"
        );

        // testing internal assertions of `+`
        let _ = self.header.free_start + self.data.len() as u16;

        let mut entries = Vec::<&Entry<K::Archived, V::Archived>>::with_capacity(
            self.header.slot_len.value() as usize,
        );

        for &slot in self.slots.iter() {
            let entry = self.get_by_slot(slot);
            entries.push(entry);
        }

        assert!(entries.is_sorted());
        // should only be checking keys for dups
        // entries.dedup();
        // assert_eq!(entries.len(), self.header.slot_len.value() as usize, "duplicate keys");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive)]
#[archive(as = "Self")]
pub(crate) struct Slot {
    /// The offset of the entry from the start of the page
    offset: Offset,
    flags: SlotFlags,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive)]
#[archive(as = "Self")]
#[repr(transparent)]
pub(crate) struct SlotFlags([u8; 2]);

impl fmt::Debug for SlotFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotFlags").field("length", &self.length()).finish()
    }
}

impl SlotFlags {
    const LENGTH_BITS: u16 = 12;
    const LENGTH_MASK: u16 = (1 << Self::LENGTH_BITS) - 1;

    fn new(length: u16) -> Self {
        assert!(length <= 1 << Self::LENGTH_BITS, "length `{length}` is too large");
        Self(u16::to_be_bytes(length))
    }

    const fn length(self) -> u16 {
        let len = u16::from_be_bytes(self.0) & Self::LENGTH_MASK;
        debug_assert!(len <= 1 << Self::LENGTH_BITS);
        len
    }

    fn set(&mut self, idx: usize) {
        assert!(idx < 4, "only high 4 bits are reserved for flags");
        let prev_len = self.length();
        self.0[0] |= 1 << (7 - idx);
        debug_assert_eq!(prev_len, self.length(), "setting flag changed the length");
        debug_assert!(self.is_set(idx), "setting flag failed");
    }

    fn is_set(&self, idx: usize) -> bool {
        assert!(idx < 4, "only high 4 bits are reserved for flags");
        self.0[0] & (1 << (7 - idx)) != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, rkyv::Serialize)]
#[archive(as = "Self")]
#[repr(transparent)]
pub(crate) struct Offset(BigEndian<u16>);

impl Add<u16> for Offset {
    type Output = Self;

    #[track_caller]
    fn add(self, rhs: u16) -> Self::Output {
        Self::from(self.0 + rhs)
    }
}

impl AddAssign<u16> for Offset {
    fn add_assign(&mut self, rhs: u16) {
        self.0 += rhs;
    }
}

impl SubAssign<u16> for Offset {
    fn sub_assign(&mut self, rhs: u16) {
        self.0 -= rhs;
    }
}

impl From<usize> for Offset {
    fn from(offset: usize) -> Self {
        Self::from(offset as u16)
    }
}

impl From<u16> for Offset {
    #[track_caller]
    fn from(offset: u16) -> Self {
        assert!(offset < PAGE_DATA_SIZE as u16, "offset `{offset}` is too large");
        Self(BigEndian::from(offset))
    }
}

impl Sub for Offset {
    type Output = u16;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}

impl Sub<u16> for Offset {
    type Output = Offset;

    #[track_caller]
    fn sub(self, rhs: u16) -> Self::Output {
        Self::from(self.0 - rhs)
    }
}
