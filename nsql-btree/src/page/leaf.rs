use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_rkyv::DefaultSerializer;
use nsql_util::static_assert_eq;
use rkyv::option::ArchivedOption;
use rkyv::{Archive, Archived, Serialize};

use super::node::{Extra, NodeHeader, NodeView, NodeViewMut};
use super::slotted::SlottedPageViewMut;
use super::{Flags, NodeMut};
use crate::btree::ConcurrentSplit;
use crate::page::slotted::SlottedPageView;
use crate::page::PageHeader;
use crate::Result;

const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

#[derive(Debug, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, PartialEq))]
pub(crate) struct LeafPageHeader {
    magic: [u8; 4],
    left_link: Option<PageIndex>,
    right_link: Option<PageIndex>,
}

impl NodeHeader for Archived<LeafPageHeader> {
    fn set_left_link(&mut self, left_link: PageIndex) {
        self.left_link = ArchivedOption::Some(left_link.into());
    }

    fn set_right_link(&mut self, right_link: PageIndex) {
        self.right_link = ArchivedOption::Some(right_link.into());
    }

    fn right_link(&self) -> Option<PageIndex> {
        self.right_link.as_ref().map(|&idx| idx.into())
    }
}

impl ArchivedLeafPageHeader {
    fn check_magic(&self) -> Result<()> {
        if self.magic != BTREE_LEAF_PAGE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree leaf page header magic: {:#x?}", &self.magic[..]),
            ))?;
        }
        Ok(())
    }
}

impl Default for LeafPageHeader {
    fn default() -> Self {
        Self { magic: BTREE_LEAF_PAGE_MAGIC, left_link: None, right_link: None }
    }
}

#[repr(C)]
pub(crate) struct LeafPageView<'a, K: Archive + 'static, V: Archive> {
    page_header: &'a Archived<PageHeader>,
    header: &'a Archived<LeafPageHeader>,
    slotted_page: SlottedPageView<'a, K, V, LeafExtra<K>>,
}

#[derive(Debug, Archive, Serialize)]
#[repr(C)]
pub(crate) struct LeafExtra<K> {
    high_key: Option<K>,
}

impl<K: Archive> Extra<K> for ArchivedLeafExtra<K> {
    fn high_key(&self) -> &Archived<Option<K>> {
        &self.high_key
    }

    fn set_high_key(&mut self, high_key: Archived<Option<K>>) {
        self.high_key = high_key;
    }
}

impl<K, V> fmt::Debug for LeafPageView<'_, K, V>
where
    K: Archive,
    K::Archived: fmt::Debug + Ord,
    V: Archive,
    V::Archived: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafPageView")
            .field("header", &self.header)
            .field("slotted_page", &self.slotted_page)
            .finish()
    }
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    pub(crate) unsafe fn view(data: &'a [u8; PAGE_DATA_SIZE]) -> Result<LeafPageView<'a, K, V>> {
        let (page_header_bytes, data) = data.split_array_ref();
        let page_header = unsafe { nsql_rkyv::archived_root::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::view(data);
        Ok(Self { page_header, header, slotted_page })
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn get<Q>(&self, key: &Q) -> Result<Option<&V::Archived>, ConcurrentSplit>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized + fmt::Debug,
    {
        self.ensure_can_contain(key)?;
        Ok(self.slotted_page.get(key))
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn find_min<Q>(
        &self,
        lower_bound: &Q,
    ) -> Result<Option<&V::Archived>, ConcurrentSplit>
    where
        K::Archived: PartialOrd<Q>,
        Q: ?Sized + fmt::Debug,
    {
        self.ensure_can_contain(lower_bound)?;
        Ok(self.slotted_page.find_min(lower_bound))
    }
}

#[repr(C)]
pub(crate) struct LeafPageViewMut<'a, K: Archive + 'static, V> {
    page_header: Pin<&'a mut Archived<PageHeader>>,
    header: Pin<&'a mut Archived<LeafPageHeader>>,
    slotted_page: SlottedPageViewMut<'a, K, V, LeafExtra<K>>,
}

impl<'a, K: Archive + 'static, V: Archive + 'static> Deref for LeafPageViewMut<'a, K, V> {
    type Target = LeafPageView<'a, K, V>;

    fn deref(&self) -> &Self::Target {
        static_assert_eq!(
            std::mem::size_of::<LeafPageViewMut<'a, (), ()>>(),
            std::mem::size_of::<LeafPageView<'a, (), ()>>()
        );
        let this = unsafe { &*(self as *const _ as *const Self::Target) };
        debug_assert_eq!(&*self.page_header, this.page_header);
        debug_assert_eq!(&*self.header, this.header);
        this
    }
}

impl<'a, K, V> NodeView<'a, K, V> for LeafPageView<'a, K, V>
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    type ArchivedNodeHeader = Archived<LeafPageHeader>;

    type Extra = LeafExtra<K>;

    fn slotted_page(&self) -> &SlottedPageView<'a, K, V, Self::Extra> {
        &self.slotted_page
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        self.page_header
    }

    fn node_header(&self) -> &Self::ArchivedNodeHeader {
        self.header
    }

    fn high_key(&self) -> &Archived<Option<K>> {
        let high_key = &self.slotted_page.extra().high_key;
        assert!(
            high_key.is_some() != self.is_rightmost(),
            "high key should exist iff not rightmost: high_key={:?}, rightmost={:?}",
            high_key,
            self.is_rightmost()
        );
        high_key
    }

    fn min_key(&self) -> Option<&K::Archived> {
        self.slotted_page.first()
    }
}

impl<'a, K, V> NodeView<'a, K, V> for LeafPageViewMut<'a, K, V>
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    type ArchivedNodeHeader = Archived<LeafPageHeader>;
    type Extra = LeafExtra<K>;

    fn slotted_page(&self) -> &SlottedPageView<'a, K, V, Self::Extra> {
        (**self).slotted_page()
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        (**self).page_header()
    }

    fn node_header(&self) -> &Self::ArchivedNodeHeader {
        (**self).node_header()
    }

    fn high_key(&self) -> &Archived<Option<K>> {
        (**self).high_key()
    }

    fn min_key(&self) -> Option<&K::Archived> {
        (**self).min_key()
    }
}

impl<K, V> NodeMut<K, V> for LeafPageViewMut<'_, K, V>
where
    K: Serialize<DefaultSerializer> + fmt::Debug + 'static,
    K::Archived: Ord + fmt::Debug,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    type ViewMut<'a> = LeafPageViewMut<'a, K, V>;

    unsafe fn view_mut(data: &mut [u8; nsql_pager::PAGE_DATA_SIZE]) -> Result<Self::ViewMut<'_>> {
        let (page_header_bytes, data) = data.split_array_mut();
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        let header = nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes);
        header.check_magic()?;
        let slotted_page = SlottedPageViewMut::view_mut(data);
        Ok(LeafPageViewMut { page_header, header, slotted_page })
    }

    fn initialize_with_flags(
        flags: Flags,
        data: &mut [u8; nsql_pager::PAGE_DATA_SIZE],
    ) -> Self::ViewMut<'_> {
        data.fill(0);
        let (page_header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(page_header_bytes, &PageHeader::new(flags | Flags::IS_LEAF));
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &LeafPageHeader::default());
        let header = unsafe { nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes) };
        header.check_magic().expect("sanity check");

        let slotted_page = SlottedPageViewMut::initialize(data, LeafExtra { high_key: None });

        LeafPageViewMut { page_header, header, slotted_page }
    }
}

impl<'a, K, V> NodeViewMut<'a, K, V> for LeafPageViewMut<'a, K, V>
where
    K: Archive + fmt::Debug + 'static,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug + 'static,
    V::Archived: fmt::Debug,
{
    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, K, V, Self::Extra> {
        &mut self.slotted_page
    }

    unsafe fn page_header_mut(&mut self) -> Pin<&mut Archived<PageHeader>> {
        self.page_header.as_mut()
    }

    fn node_header_mut(&mut self) -> Pin<&mut Self::ArchivedNodeHeader> {
        self.header.as_mut()
    }

    fn set_high_key(&mut self, high_key: Archived<Option<K>>) {
        assert!(
            self.is_rightmost() != high_key.is_some(),
            "rightmost page should not have a high key (high_key: {high_key:?})",
        );
        unsafe { self.slotted_page.extra_mut().get_unchecked_mut() }.set_high_key(high_key)
    }
}
