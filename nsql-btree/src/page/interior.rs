use std::ops::Deref;
use std::pin::Pin;
use std::{fmt, io, mem};

use nsql_pager::PageIndex;
use nsql_util::static_assert_eq;
use rkyv::{Archive, Archived};

use super::slotted::{SlottedPageView, SlottedPageViewMut};
use super::{PageFull, PageHeader};
use crate::page::archived_size_of;
use crate::Rkyv;

const BTREE_INTERIOR_PAGE_MAGIC: [u8; 4] = *b"BTPI";

#[derive(Debug, PartialEq, Archive, rkyv::Serialize)]
#[archive_attr(derive(Debug))]
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
    header: &'a Archived<InteriorPageHeader>,
    slotted_page: SlottedPageView<'a, K, PageIndex>,
}

impl<'a, K> InteriorPageView<'a, K>
where
    K: Rkyv + fmt::Debug,
    K::Archived: fmt::Debug + Ord + PartialOrd<K>,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8],
    ) -> nsql_serde::Result<InteriorPageView<'a, K>> {
        let (header_bytes, data) = data.split_array_ref();
        let header = nsql_rkyv::archived_root::<InteriorPageHeader>(header_bytes);
        header.check_magic()?;

        let slotted_page = SlottedPageView::create(data).await?;
        Ok(Self { header, slotted_page })
    }

    pub(crate) async fn search(&self, key: &K::Archived) -> nsql_serde::Result<PageIndex> {
        // FIXME add logic to handle the special case of the lowest key
        let slot_idx = match self.slotted_page.slot_index_of(key).await? {
            // special case of the lowest key being stored in the rightmost slot
            Err(idx) if idx == 0 => self.slotted_page.slots().len() - 1,
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        };

        let slot = self.slotted_page.slots()[slot_idx];

        let (_, page_idx) = self.slotted_page.get_by_slot(slot).await?;
        Ok(PageIndex::from(*page_idx))
    }

    // a node has a low key iff it is not the left-most node
    pub(crate) fn low_key(&self) -> nsql_serde::Result<&K::Archived> {
        let slot =
            self.slotted_page.slots().last().expect("interior slots should always be non-empty");
        self.slotted_page.key_in_slot(slot)
    }

    fn is_leftmost(&self) -> bool {
        self.header.left_link.is_none()
    }
}

// NOTE: must have the same layout as `InteriorPageView`
#[repr(C)]
pub(crate) struct InteriorPageViewMut<'a, K> {
    header: Pin<&'a mut Archived<InteriorPageHeader>>,
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
        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &InteriorPageHeader::default());
        // the slots start after the page header and the interior page header
        let prefix_size = archived_size_of!(PageHeader) + archived_size_of!(InteriorPageHeader);
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
    K: Ord + Rkyv + fmt::Debug,
    K::Archived: fmt::Debug + Ord + PartialOrd<K>,
{
    pub(crate) async fn insert(
        &mut self,
        sep: &K::Archived,
        page_idx: PageIndex,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        assert!(
            self.slotted_page.slots().len() > 1,
            "can only use `insert` operation on a non-empty node"
        );

        let low_key = self.low_key()?;
        assert!(low_key < sep);

        // FIXME is this even right?
        match self.slotted_page.insert(sep, &page_idx.into()).await? {
            Ok(()) => Ok(Ok(())),
            Err(PageFull) => Ok(Err(PageFull)),
        }
    }

    pub(crate) async fn insert_initial(
        &mut self,
        low_key: &K::Archived,
        left: PageIndex,
        sep: &K::Archived,
        right: PageIndex,
    ) -> nsql_serde::Result<Result<(), PageFull>> {
        assert!(
            self.slotted_page.slots().is_empty(),
            "can only use this operation on an empty node as the initial insert"
        );

        match self.slotted_page.insert(low_key, &left.into()).await? {
            Ok(()) => {}
            Err(PageFull) => unreachable!("page should not be full after a single insert"),
        };

        match self.slotted_page.insert(sep, &right.into()).await? {
            Ok(()) => {}
            Err(PageFull) => unreachable!("page should not be full after two inserts"),
        };

        Ok(Ok(()))
    }

    // FIXME we need to split left not right and set the left link
    pub(crate) async fn split_into<'r>(
        &mut self,
        new: &mut InteriorPageViewMut<'r, K>,
    ) -> nsql_serde::Result<&'r K::Archived> {
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

    pub(crate) async fn split_root_into(
        &mut self,
        left_page_idx: PageIndex,
        left_child: &mut InteriorPageViewMut<'_, K>,
        right_child: &mut InteriorPageViewMut<'_, K>,
    ) -> nsql_serde::Result<()> {
        assert!(left_child.slotted_page.is_empty());
        assert!(right_child.slotted_page.is_empty());
        assert!(self.slotted_page.len() > 1);

        right_child.header.left_link = nsql_rkyv::to_archive(Some(left_page_idx));

        let slots = self.slotted_page.slots();
        let (lhs, rhs) = slots.split_at(slots.len() / 2);
        for &slot in lhs {
            let (key, value) = self.slotted_page.get_by_slot(slot).await?;
            // using internal insert to avoid assertions that don't yet hold
            left_child.slotted_page.insert(key, value).await?.unwrap();
        }

        for &slot in rhs {
            let (key, value) = self.slotted_page.get_by_slot(slot).await?;
            right_child.slotted_page.insert(key, value).await?.unwrap();
        }

        self.slotted_page.set_len(0);

        Ok(())
    }
}
