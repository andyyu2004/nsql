use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io};

use nsql_pager::PageIndex;
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived};

use super::slotted::SlottedPageViewMut;
use super::{ArchivedKeyValuePair, KeyValuePair, PageFull};
use crate::page::slotted::SlottedPageView;
use crate::page::{archived_size_of, PageHeader};

const BTREE_LEAF_PAGE_MAGIC: [u8; 4] = *b"BTPL";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug))]
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
    header: &'a Archived<LeafPageHeader>,
    slotted_page: SlottedPageView<'a, KeyValuePair<K, V>>,
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
{
    pub(crate) unsafe fn create(data: &'a [u8]) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::<'a, KeyValuePair<K, V>>::create(data);
        Ok(Self { header, slotted_page })
    }

    pub(crate) async fn get(&self, key: &K::Archived) -> Option<&V::Archived> {
        self.slotted_page.get(key).map(|kv| &kv.value)
    }

    pub(crate) fn low_key(&self) -> &K::Archived {
        &self.slotted_page.low_key().key
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct LeafPageViewMut<'a, K, V> {
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
        unsafe { &*(self as *const _ as *const Self::Target) }
    }
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    /// initialize a new leaf page
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &LeafPageHeader::default());
        let header = unsafe { nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes) };

        // the slots start after the page header and the leaf page header
        let prefix_size = archived_size_of!(PageHeader) + archived_size_of!(LeafPageHeader);
        let slotted_page =
            SlottedPageViewMut::<'a, KeyValuePair<K, V>>::init(data, prefix_size).await?;

        Ok(Self { header, slotted_page })
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) = data.split_array_mut();
        let header = nsql_rkyv::archived_root_mut::<LeafPageHeader>(header_bytes);
        header.check_magic()?;
        let slotted_page = SlottedPageViewMut::create(data).await?;
        Ok(Self { header, slotted_page })
    }
}

impl<'a, K, V> LeafPageViewMut<'a, K, V>
where
    K: Archive + Ord + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
{
    pub(crate) fn insert(&mut self, key: K::Archived, value: V::Archived) -> Result<(), PageFull> {
        self.insert_kv(&ArchivedKeyValuePair { key, value })
    }

    pub(crate) fn insert_kv(&mut self, kv: &ArchivedKeyValuePair<K, V>) -> Result<(), PageFull> {
        self.slotted_page.insert(kv)
    }

    // FIXME we need to split left not right and set the left link
    pub(crate) async fn split_into(
        &mut self,
        new: &mut LeafPageViewMut<'_, K, V>,
    ) -> nsql_serde::Result<()> {
        assert!(new.slotted_page.is_empty());
        assert!(self.slotted_page.len() > 1);

        let slots = self.slotted_page.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);

        for &slot in rhs {
            let value = self.slotted_page.get_by_slot(slot);
            new.slotted_page.insert(value).expect("new page should not be full");
        }

        self.slotted_page.set_len(lhs.len() as u16);
        Ok(())
    }

    /// Intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    pub(crate) async fn split_root_into(
        &mut self,
        left: &mut LeafPageViewMut<'_, K, V>,
        right: &mut LeafPageViewMut<'_, K, V>,
    ) -> nsql_serde::Result<()> {
        assert!(left.slotted_page.is_empty());
        assert!(right.slotted_page.is_empty());
        assert!(self.slotted_page.len() > 1);

        let slots = self.slotted_page.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for &slot in lhs {
            let value = self.slotted_page.get_by_slot(slot);
            left.insert_kv(value).unwrap();
        }

        for &slot in rhs {
            let value = self.slotted_page.get_by_slot(slot);
            right.insert_kv(value).unwrap();
        }

        self.slotted_page.set_len(0);
        Ok(())
    }
}
