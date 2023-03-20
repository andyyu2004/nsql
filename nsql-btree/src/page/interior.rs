use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io, mem};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_rkyv::DefaultSerializer;
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived, Serialize};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{Flags, KeyValuePair, Node, NodeMut, PageFull, PageHeader};
use crate::page::archived_size_of;
use crate::Result;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
pub(crate) struct InteriorPageHeader {
    magic: [u8; 4],
    left_link: Option<PageIndex>,
}

impl Default for InteriorPageHeader {
    fn default() -> Self {
        Self { magic: BTREE_INTERIOR_PAGE_MAGIC, left_link: None }
    }
}

impl ArchivedInteriorPageHeader {
    fn check_magic(&self) -> nsql_serde::Result<()> {
        if self.magic != BTREE_INTERIOR_PAGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree interior page header magic: {:#x?}", &self.magic[..]),
            ))?;
        }
        Ok(())
    }
}

// NOTE: must have the same layout as `InteriorPageViewMut`
#[repr(C)]
pub(crate) struct InteriorPageView<'a, K> {
    page_header: &'a Archived<PageHeader>,
    header: &'a Archived<InteriorPageHeader>,
    slotted_page: SlottedPageView<'a, KeyValuePair<K, PageIndex>>,
}

impl<K> fmt::Debug for InteriorPageView<'_, K>
where
    K: Archive,
    K::Archived: fmt::Debug + Ord,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafPageView")
            .field("header", &self.header)
            .field("slotted_page", &self.slotted_page)
            .finish()
    }
}

impl<'a, K> InteriorPageView<'a, K>
where
    K: Archive,
    K::Archived: Ord + fmt::Debug,
{
    pub(crate) unsafe fn view(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<InteriorPageView<'a, K>> {
        let (page_header_bytes, data) = data.split_array_ref();
        let page_header = nsql_rkyv::archived_root::<PageHeader>(page_header_bytes);

        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<InteriorPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::view(data);
        Ok(Self { page_header, header, slotted_page })
    }

    pub(crate) fn search(&self, key: &K::Archived) -> PageIndex {
        let slot_idx = match self.slotted_page.slot_index_of_key(key) {
            Err(idx) if idx == 0 => todo!("key was lowest than the low key"),
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };

        let slot = self.slotted_page.slots()[slot_idx];

        let kv = self.slotted_page.get_by_slot(slot);
        PageIndex::from(kv.value)
    }

    fn is_leftmost(&self) -> bool {
        self.header.left_link.is_none()
    }
}

// NOTE: must have the same layout as `InteriorPageView`
#[repr(C)]
pub(crate) struct InteriorPageViewMut<'a, K> {
    page_header: Pin<&'a mut Archived<PageHeader>>,
    header: Pin<&'a mut Archived<InteriorPageHeader>>,
    slotted_page: SlottedPageViewMut<'a, KeyValuePair<K, PageIndex>>,
}

impl<K> fmt::Debug for InteriorPageViewMut<'_, K>
where
    K: Archive,
    K::Archived: fmt::Debug + Ord,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<'a, K> Deref for InteriorPageViewMut<'a, K> {
    type Target = InteriorPageView<'a, K>;

    fn deref(&self) -> &Self::Target {
        static_assert_eq!(
            mem::size_of::<InteriorPageView<'a, ()>>(),
            mem::size_of::<InteriorPageViewMut<'a, ()>>()
        );
        // SAFETY: the only difference between InteriorPageView and InteriorPageViewMut is the mutability of the pointers
        // the layout is identical
        let this = unsafe { &*(self as *const _ as *const Self::Target) };
        debug_assert_eq!(&*self.page_header, this.page_header);
        debug_assert_eq!(&*self.header, this.header);
        this
    }
}

impl<'a, K> InteriorPageViewMut<'a, K>
where
    K: Ord + Archive + Serialize<DefaultSerializer> + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    pub(crate) async fn insert(
        &mut self,
        sep: K::Archived,
        page_idx: PageIndex,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        let kv = Archived::<KeyValuePair<K, PageIndex>>::new(sep, page_idx.into());

        if let Some(low_key) = self.low_key() {
            assert!(low_key < &kv.key);
        }

        match self.slotted_page.insert(&kv) {
            Ok(()) => Ok(Ok(())),
            Err(PageFull) => Ok(Err(PageFull)),
        }
    }

    pub(crate) fn insert_initial(
        &mut self,
        low_key: K::Archived,
        left: PageIndex,
        sep: K::Archived,
        right: PageIndex,
    ) -> Result<(), PageFull> {
        assert!(
            self.slotted_page.slots().is_empty(),
            "can only use this operation on an empty node as the initial insert"
        );

        let left = Archived::<KeyValuePair<K, PageIndex>>::new(low_key, left.into());
        match self.slotted_page.insert(&left) {
            Ok(()) => {}
            Err(PageFull) => unreachable!("page should not be full after a single insert"),
        };

        let right = Archived::<KeyValuePair<K, PageIndex>>::new(sep, right.into());
        match self.slotted_page.insert(&right) {
            Ok(()) => {}
            Err(PageFull) => unreachable!("page should not be full after two inserts"),
        };

        Ok(())
    }

    // FIXME we need to split left not right and set the left link
    pub(crate) fn split_into(&mut self, new: &mut InteriorPageViewMut<'_, K>) {
        assert!(new.slotted_page.is_empty());
        assert!(self.slotted_page.len() > 1);

        let slots = self.slotted_page.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);

        for &slot in rhs {
            let value = self.slotted_page.get_by_slot(slot);
            new.slotted_page.insert(value).expect("new page should not be full");
        }

        self.slotted_page.set_len(lhs.len() as u16);
    }
}

impl<'a, K> Node<'a, K, PageIndex> for InteriorPageView<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, PageIndex>> {
        &self.slotted_page
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        self.page_header
    }

    fn low_key(&self) -> Option<&K::Archived> {
        self.slotted_page.first().map(|kv| &kv.key)
    }
}

impl<'a, K> Node<'a, K, PageIndex> for InteriorPageViewMut<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, PageIndex>> {
        (**self).slotted_page()
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        (**self).page_header()
    }

    fn low_key(&self) -> Option<&K::Archived> {
        (**self).low_key()
    }
}

impl<'a, K> NodeMut<'a, K, PageIndex> for InteriorPageViewMut<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    fn initialize_with_flags(flags: Flags, data: &'a mut [u8; PAGE_DATA_SIZE]) -> Self {
        assert!(!flags.contains(Flags::IS_LEAF), "tried to init an interior page as a leaf");
        data.fill(0);
        let (page_header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(page_header_bytes, &PageHeader::new(flags));
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &InteriorPageHeader::default());

        // the slots start after the page header and the interior page header
        let prefix_size = archived_size_of!(PageHeader) + archived_size_of!(InteriorPageHeader);
        let slotted_page =
            SlottedPageViewMut::<'a, KeyValuePair<K, PageIndex>>::init(data, prefix_size);

        let header = unsafe { nsql_rkyv::archived_root_mut::<InteriorPageHeader>(header_bytes) };
        header.check_magic().expect("magic should be correct as we just set it");

        Self { page_header, header, slotted_page }
    }

    unsafe fn view_mut(data: &'a mut [u8; PAGE_DATA_SIZE]) -> Result<Self> {
        let (page_header_bytes, data) = data.split_array_mut();
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        let header = nsql_rkyv::archived_root_mut::<InteriorPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageViewMut::<'a, KeyValuePair<K, PageIndex>>::view_mut(data);

        Ok(Self { page_header, header, slotted_page })
    }

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, KeyValuePair<K, PageIndex>> {
        &mut self.slotted_page
    }

    unsafe fn page_header_mut(&mut self) -> Pin<&mut Archived<PageHeader>> {
        self.page_header.as_mut()
    }
}
