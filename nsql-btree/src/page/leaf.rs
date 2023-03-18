use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io};

use nsql_pager::PageIndex;
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived};

use super::slotted::SlottedPageViewMut;
use super::PageFull;
use crate::page::slotted::SlottedPageView;
use crate::page::{archived_size_of, PageHeader};
use crate::Rkyv;

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
    slotted_page: SlottedPageView<'a, K, V>,
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Rkyv + fmt::Debug,
    K::Archived: fmt::Debug + Ord + PartialOrd<K>,
    V: Rkyv + fmt::Debug,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8],
    ) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::<'a, K, V>::create(data).await?;
        Ok(Self { header, slotted_page })
    }

    pub(crate) async fn get(&self, key: &K::Archived) -> nsql_serde::Result<Option<&V::Archived>> {
        self.slotted_page.get(key).await
    }

    pub(crate) fn low_key(&self) -> nsql_serde::Result<&K::Archived> {
        self.slotted_page.low_key()
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct LeafPageViewMut<'a, K, V> {
    header: Pin<&'a mut Archived<LeafPageHeader>>,
    slotted_page: SlottedPageViewMut<'a, K, V>,
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
        let slotted_page = SlottedPageViewMut::<'a, K, V>::init(data, prefix_size).await?;

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
    K: Rkyv + Ord + fmt::Debug,
    K::Archived: fmt::Debug + Ord + PartialOrd<K>,
    V: Rkyv + fmt::Debug,
{
    pub(crate) async fn insert(
        &mut self,
        key: &K::Archived,
        value: &V::Archived,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        self.slotted_page.insert(key, value).await
    }

    // FIXME we need to split left not right and set the left link
    pub(crate) async fn split_into(
        &mut self,
        new: &mut LeafPageViewMut<'_, K, V>,
    ) -> nsql_serde::Result<&K::Archived> {
        assert!(new.slotted_page.is_empty());
        assert!(self.slotted_page.len() > 1);

        let slots = self.slotted_page.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);

        let mut sep = None;
        for &slot in rhs {
            let (key, value) = self.slotted_page.get_by_slot(slot).await?;
            new.slotted_page.insert(key, value).await?.expect("new page should not be full");
            if sep.is_none() {
                sep = Some(key);
            }
        }

        self.slotted_page.set_len(lhs.len() as u16);

        todo!();
        // Ok(sep.unwrap())
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
            let (key, value) = self.slotted_page.get_by_slot(slot).await?;
            left.insert(key, value).await?.unwrap();
        }

        for &slot in rhs {
            let (key, value) = self.slotted_page.get_by_slot(slot).await?;
            right.insert(key, value).await?.unwrap();
        }

        self.slotted_page.set_len(0);
        Ok(())
    }
}
