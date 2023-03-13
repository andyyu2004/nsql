use std::pin::Pin;
use std::{fmt, io, mem};

use nsql_pager::PageIndex;
use nsql_serde::{Deserialize, DeserializeSkip, Serialize, SerializeSized};
use rkyv::Archive;

use super::slotted::SlottedPageViewMut;
use super::PageFull;
use crate::page::slotted::SlottedPageView;
use crate::page::PageHeader;

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

pub(crate) struct LeafPageView<'a, K, V> {
    header: &'a ArchivedLeafPageHeader,
    slots: SlottedPageView<'a, K, V>,
}

impl<'a, K, V> LeafPageView<'a, K, V>
where
    K: Ord + DeserializeSkip,
    V: Deserialize,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8],
    ) -> nsql_serde::Result<LeafPageView<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_ref::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = rkyv::archived_root::<LeafPageHeader>(header_bytes);
        header.check_magic()?;

        let slots = SlottedPageView::<'a, K, V>::create(data).await?;
        Ok(Self { header, slots })
    }

    pub(crate) async fn get(&self, key: &K) -> nsql_serde::Result<Option<V>> {
        self.slots.get(key).await
    }
}

#[derive(Debug)]
pub(crate) struct LeafPageViewMut<'a, K, V> {
    header: Pin<&'a mut ArchivedLeafPageHeader>,
    slotted: SlottedPageViewMut<'a, K, V>,
}

impl<'a, K, V> LeafPageViewMut<'a, K, V> {
    pub(crate) fn slotted(&self) -> &SlottedPageViewMut<'a, K, V> {
        &self.slotted
    }

    /// initialize a new leaf page
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<()> {
        const HEADER_SIZE: u16 = mem::size_of::<ArchivedLeafPageHeader>() as u16;
        let (header_bytes, data) = data.split_array_mut::<{ HEADER_SIZE as usize }>();
        header_bytes.copy_from_slice(&nsql_rkyv::archive(&LeafPageHeader::default()));
        // the slots start after the page header and the leaf page header
        let prefix_size = PageHeader::SERIALIZED_SIZE + HEADER_SIZE;
        SlottedPageViewMut::<'a, K, V>::init(data, prefix_size).await?;

        Ok(())
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        let (header_bytes, data) =
            data.split_array_mut::<{ mem::size_of::<ArchivedLeafPageHeader>() }>();
        let header = rkyv::archived_root_mut::<LeafPageHeader>(Pin::new(header_bytes));
        header.check_magic()?;
        let slots = SlottedPageViewMut::create(data).await?;
        Ok(Self { header, slotted: slots })
    }

    fn downgrade(&'a self) -> LeafPageView<'a, K, V> {
        LeafPageView { header: &self.header, slots: self.slotted.downgrade() }
    }
}

impl<'a, K, V> LeafPageViewMut<'a, K, V>
where
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
    V: Serialize + Deserialize + fmt::Debug,
{
    pub(crate) async fn insert(
        &mut self,
        key: K,
        value: V,
    ) -> nsql_serde::Result<Result<Option<V>, PageFull>> {
        self.slotted.insert(key, value).await
    }

    /// Intended for use when splitting a root node.
    /// We keep the root node page number unchanged because it may be referenced as an identifier.
    /// We allocate two new nodes to move the data into.
    pub(crate) async fn split_root_into(
        &self,
        mut left: LeafPageViewMut<'a, K, V>,
        mut right: LeafPageViewMut<'a, K, V>,
    ) -> nsql_serde::Result<()> {
        assert!(left.slotted.downgrade().is_empty());
        assert!(right.slotted.downgrade().is_empty());

        let slots = self.slotted.downgrade().slots();
        // FIXME use less naive algorithm for inserting into new pages
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for slot in lhs {
            let (key, value) = self.downgrade().slots.get_by_offset(slot.offset()).await?;
            left.insert(key, value).await?.unwrap();
        }

        for slot in rhs {
            let (key, value) = self.downgrade().slots.get_by_offset(slot.offset()).await?;
            right.insert(key, value).await?.unwrap();
        }

        Ok(())
    }
}
