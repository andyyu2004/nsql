use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io};

use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived};

use super::node::Node;
use super::slotted::SlottedPageViewMut;
use super::{Flags, KeyValuePair, NodeMut};
use crate::page::slotted::SlottedPageView;
use crate::page::{archived_size_of, PageHeader};
use crate::Result;

const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug, PartialEq))]
pub(crate) struct LeafPageHeader {
    magic: [u8; 4],
    prev: Option<PageIndex>,
    next: Option<PageIndex>,
}

impl ArchivedLeafPageHeader {
    fn check_magic(&self) -> nsql_serde::Result<()> {
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
        Self { magic: BTREE_LEAF_PAGE_MAGIC, prev: None, next: None }
    }
}

#[repr(C)]
pub(crate) struct LeafPageView<'a, K, V> {
    page_header: &'a Archived<PageHeader>,
    header: &'a Archived<LeafPageHeader>,
    slotted_page: SlottedPageView<'a, KeyValuePair<K, V>>,
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
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    pub(crate) unsafe fn view(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let (page_header_bytes, data) = data.split_array_ref();
        let page_header = unsafe { nsql_rkyv::archived_root::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::<'a, KeyValuePair<K, V>>::view(data);
        Ok(Self { page_header, header, slotted_page })
    }

    pub(crate) fn get(&self, key: &K::Archived) -> Option<&V::Archived> {
        self.slotted_page.get(key).map(|kv| &kv.value)
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct LeafPageViewMut<'a, K, V> {
    page_header: Pin<&'a mut Archived<PageHeader>>,
    header: Pin<&'a mut Archived<LeafPageHeader>>,
    slotted_page: SlottedPageViewMut<'a, KeyValuePair<K, V>>,
}

impl<'a, K, V> Deref for LeafPageViewMut<'a, K, V> {
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

impl<'a, K, V> LeafPageViewMut<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    // FIXME we need to split left not right and set the left link
    pub(crate) fn split_into(&mut self, new: &mut LeafPageViewMut<'_, K, V>) {}
}

impl<'a, K, V> Node<'a, K, V> for LeafPageView<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, V>> {
        &self.slotted_page
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        self.page_header
    }

    fn low_key(&self) -> Option<&K::Archived> {
        self.slotted_page.first().map(|kv| &kv.key)
    }
}

impl<'a, K, V> Node<'a, K, V> for LeafPageViewMut<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    fn slotted_page(&self) -> &SlottedPageView<'a, KeyValuePair<K, V>> {
        (**self).slotted_page()
    }

    fn page_header(&self) -> &Archived<PageHeader> {
        (**self).page_header()
    }

    fn low_key(&self) -> Option<&K::Archived> {
        (**self).low_key()
    }
}

impl<'a, K, V> NodeMut<'a, K, V> for LeafPageViewMut<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    fn initialize_with_flags(flags: Flags, data: &'a mut [u8; nsql_pager::PAGE_DATA_SIZE]) -> Self {
        data.fill(0);
        let (page_header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(page_header_bytes, &PageHeader::new(flags | Flags::IS_LEAF));
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &LeafPageHeader::default());
        let header = unsafe { nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes) };
        header.check_magic().expect("sanity check");

        // the slots start after the page header and the leaf page header
        let prefix_size = archived_size_of!(PageHeader) + archived_size_of!(LeafPageHeader);
        let slotted_page = SlottedPageViewMut::<'a, KeyValuePair<K, V>>::init(data, prefix_size);

        Self { page_header, header, slotted_page }
    }

    unsafe fn view_mut(data: &'a mut [u8; nsql_pager::PAGE_DATA_SIZE]) -> Result<Self> {
        let (page_header_bytes, data) = data.split_array_mut();
        let page_header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(page_header_bytes) };

        let (header_bytes, data) = data.split_array_mut();
        let header = nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes);
        header.check_magic()?;
        let slotted_page = SlottedPageViewMut::view_mut(data);
        Ok(Self { page_header, header, slotted_page })
    }

    fn slotted_page_mut(&mut self) -> &mut SlottedPageViewMut<'a, KeyValuePair<K, V>> {
        &mut self.slotted_page
    }

    unsafe fn page_header_mut(&mut self) -> Pin<&mut Archived<PageHeader>> {
        self.page_header.as_mut()
    }
}
