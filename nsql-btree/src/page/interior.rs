use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io, mem};

use nsql_pager::PageIndex;
use nsql_serde::{DeserializeSkip, Serialize, SerializeSized};
use nsql_util::static_assert_eq;
use rkyv::Archive;

use super::slotted::{SlotOffset, SlottedPageView, SlottedPageViewMut};
use super::PageFull;
use crate::page::PageHeader;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug))]
pub(crate) struct InteriorPageHeader {
    magic: [u8; 4],
}

impl Default for InteriorPageHeader {
    fn default() -> Self {
        Self { magic: BTREE_INTERIOR_PAGE_MAGIC }
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
    header: &'a ArchivedInteriorPageHeader,
    slotted_page: SlottedPageView<'a, K, PageIndex>,
}

impl<'a, K> InteriorPageView<'a, K>
where
    K: DeserializeSkip + Ord + fmt::Debug,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8],
    ) -> nsql_serde::Result<InteriorPageView<'a, K>> {
        const HEADER_SIZE: usize = mem::size_of::<ArchivedInteriorPageHeader>();
        let (header_bytes, data) = data.split_array_ref::<{ HEADER_SIZE }>();
        let header = rkyv::archived_root::<InteriorPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::create(data).await?;
        Ok(Self { header, slotted_page })
    }

    pub(crate) async fn search(&self, key: &K) -> nsql_serde::Result<PageIndex> {
        // FIXME add logic to handle the special case of the lowest key
        let slot_idx = match self.slotted_page.slot_index_of(key).await? {
            // special case of the lowest key being stored in the rightmost slot
            Err(idx) if idx == 0 => self.slotted_page.slots().len() - 1,
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };

        let offset = self.slotted_page.slots()[slot_idx].offset();

        let (_, page_idx) = self.slotted_page.get_by_offset(offset).await?;
        Ok(page_idx)
    }
}

// NOTE: must have the same layout as `InteriorPageView`
#[repr(C)]
pub(crate) struct InteriorPageViewMut<'a, K> {
    header: Pin<&'a mut ArchivedInteriorPageHeader>,
    slotted_page: SlottedPageViewMut<'a, K, PageIndex>,
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
        unsafe { &*(&self.header as *const _ as *const Self::Target) }
    }
}

impl<'a, K> InteriorPageViewMut<'a, K> {
    /// initialize a new leaf page
    pub(crate) async fn init(data: &'a mut [u8]) -> nsql_serde::Result<InteriorPageViewMut<'a, K>> {
        const HEADER_SIZE: usize = mem::size_of::<ArchivedInteriorPageHeader>();
        let (header_bytes, data) = data.split_array_mut::<{ HEADER_SIZE }>();
        header_bytes.copy_from_slice(&nsql_rkyv::archive(&InteriorPageHeader::default()));
        // the slots start after the page header and the interior page header
        let prefix_size = PageHeader::SERIALIZED_SIZE + HEADER_SIZE as u16;
        let slotted_page = SlottedPageViewMut::<'a, K, PageIndex>::init(data, prefix_size).await?;

        let header =
            unsafe { rkyv::archived_root_mut::<InteriorPageHeader>(Pin::new(header_bytes)) };
        header.check_magic()?;

        Ok(Self { header, slotted_page })
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8],
    ) -> nsql_serde::Result<InteriorPageViewMut<'a, K>> {
        const HEADER_SIZE: usize = mem::size_of::<ArchivedInteriorPageHeader>();
        let (header_bytes, data) = data.split_array_mut::<{ HEADER_SIZE }>();
        let header = rkyv::archived_root_mut::<InteriorPageHeader>(Pin::new(header_bytes));
        header.check_magic()?;

        let slotted_page = SlottedPageViewMut::<'a, K, PageIndex>::create(data).await?;
        Ok(Self { header, slotted_page })
    }
}

impl<'a, K> InteriorPageViewMut<'a, K>
where
    K: Serialize + DeserializeSkip + Ord + fmt::Debug,
{
    pub(crate) async fn insert(
        &mut self,
        sep: &K,
        page_idx: PageIndex,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        // FIXME is this even right?
        match self.slotted_page.insert(sep, &page_idx).await? {
            Ok(Some(_)) => todo!("duplicate sep in interior?"),
            Ok(None) => Ok(Ok(())),
            Err(PageFull) => Ok(Err(PageFull)),
        }
    }

    pub(crate) async fn insert_initial(
        &mut self,
        sep: &K,
        left: PageIndex,
        right: PageIndex,
        node_high_key: &K,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        assert!(self.slotted_page.slots().is_empty());

        // confusingly, we store the leftmost pointer alongside the node_high_key in the rightmost key slot
        match self.slotted_page.insert(node_high_key, &left).await? {
            Ok(Some(_)) => todo!("duplicate sep in interior?"),
            Ok(None) => {}
            Err(PageFull) => return Ok(Err(PageFull)),
        };

        match self.slotted_page.insert(sep, &right).await? {
            Ok(Some(_)) => todo!("duplicate sep in interior?"),
            Ok(None) => {}
            Err(PageFull) => return Ok(Err(PageFull)),
        };

        Ok(Ok(()))
    }
}
