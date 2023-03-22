use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io, mem};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_util::static_assert_eq;
use rkyv::option::ArchivedOption;
use rkyv::{Archive, Archived};

use super::node::{NodeHeader, NodeView, NodeViewMut};
use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{Flags, KeyValuePair, NodeMut, PageHeader};
use crate::page::archived_size_of;
use crate::Result;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq, Eq))]
pub(crate) struct InteriorPageHeader {
    magic: [u8; 4],
    left_link: Option<PageIndex>,
}

impl NodeHeader for Archived<InteriorPageHeader> {
    fn left_link(&self) -> Archived<Option<PageIndex>> {
        self.left_link
    }

    fn set_left_link(&mut self, left_link: PageIndex) {
        self.left_link = ArchivedOption::Some(left_link.into());
    }

    fn set_right_link(&mut self, _right_link: PageIndex) {
        // interior pages don't maintain a right link, nothing to do
    }
}

impl Default for InteriorPageHeader {
    fn default() -> Self {
        Self { magic: BTREE_INTERIOR_PAGE_MAGIC, left_link: None }
    }
}

impl ArchivedInteriorPageHeader {
    fn check_magic(&self) -> Result<()> {
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
    pub(crate) unsafe fn view(data: &'a [u8; PAGE_DATA_SIZE]) -> Result<InteriorPageView<'a, K>> {
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
            Err(idx) if idx == 0 => panic!("key was lower than the low key"),
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };

        let slot = self.slotted_page.slots()[slot_idx];

        let kv = self.slotted_page.get_by_slot(slot);
        PageIndex::from(kv.value)
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

impl<'a, K> NodeView<'a, K, PageIndex> for InteriorPageView<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    type ArchivedNodeHeader = Archived<InteriorPageHeader>;

    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, PageIndex>> {
        &self.slotted_page
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        self.page_header
    }

    fn node_header(&self) -> &Self::ArchivedNodeHeader {
        self.header
    }

    fn low_key(&self) -> Option<&K::Archived> {
        (!self.is_root())
            .then(|| &self.slotted_page.first().expect("non-root should have a low_key").key)
    }
}

impl<'a, K> NodeView<'a, K, PageIndex> for InteriorPageViewMut<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
    type ArchivedNodeHeader = Archived<InteriorPageHeader>;

    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, PageIndex>> {
        (**self).slotted_page()
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        (**self).page_header()
    }

    fn node_header(&self) -> &Self::ArchivedNodeHeader {
        (**self).node_header()
    }

    fn low_key(&self) -> Option<&K::Archived> {
        (**self).low_key()
    }
}

pub struct InteriorNode<K> {
    _phantom: PhantomData<fn() -> K>,
}

impl<K> NodeMut<K, PageIndex> for InteriorNode<K>
where
    K: Archive + fmt::Debug,
    K::Archived: Ord + fmt::Debug,
{
    type ViewMut<'a> = InteriorPageViewMut<'a, K>;

    unsafe fn view_mut(data: &mut [u8; PAGE_DATA_SIZE]) -> Result<Self::ViewMut<'_>> {
        unsafe { InteriorPageViewMut::view_mut(data) }
    }

    fn initialize_with_flags(flags: Flags, data: &mut [u8; PAGE_DATA_SIZE]) -> Self::ViewMut<'_> {
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
            SlottedPageViewMut::<'_, KeyValuePair<K, PageIndex>>::init(data, prefix_size);

        let header = unsafe { nsql_rkyv::archived_root_mut::<InteriorPageHeader>(header_bytes) };
        header.check_magic().expect("magic should be correct as we just set it");

        InteriorPageViewMut { page_header, header, slotted_page }
    }
}

impl<'a, K> NodeViewMut<'a, K, PageIndex> for InteriorPageViewMut<'a, K>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
{
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

    fn node_header_mut(&mut self) -> Pin<&mut Self::ArchivedNodeHeader> {
        self.header.as_mut()
    }
}
