mod interior;
mod leaf;
mod slotted;

use std::pin::Pin;
use std::{fmt};

use nsql_pager::PAGE_DATA_SIZE;
use nsql_serde::{Deserialize, DeserializeSkip};
use rkyv::Archive;

pub(crate) use self::interior::{InteriorPageView, InteriorPageViewMut};
pub(crate) use self::leaf::{LeafPageView, LeafPageViewMut};
use crate::node::Flags;

macro_rules! archived_size_of {
    ($ty:ty) => {
        ::std::mem::size_of::<::rkyv::Archived<$ty>>() as u16
    };
}

pub(crate) use archived_size_of;

#[derive(Debug)]
pub(crate) struct PageFull;

#[derive(Debug, Archive, rkyv::Serialize)]
pub(crate) struct PageHeader {
    pub(crate) flags: Flags,
    // to make it 4-byte aligned
    padding: [u8; 3],
}

impl PageHeader {
    pub(crate) fn new(flags: Flags) -> Self {
        Self { flags, padding: [0; 3] }
    }
}

pub(crate) enum PageView<'a, K, V> {
    Interior(InteriorPageView<'a, K>),
    Leaf(LeafPageView<'a, K, V>),
}

impl<'a, K, V> PageView<'a, K, V>
where
    K: Ord + DeserializeSkip + fmt::Debug,
    V: Deserialize + fmt::Debug,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageView<'a, K, V>> {
        let (header_bytes, data) = data.split_array_ref();
        let header = unsafe { nsql_rkyv::archived_root::<PageHeader>(header_bytes) };
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageView::create(data).await.map(Self::Leaf)
        } else {
            InteriorPageView::create(data).await.map(Self::Interior)
        }
    }
}

pub(crate) struct PageViewMut<'a, K, V> {
    pub(crate) header: Pin<&'a mut ArchivedPageHeader>,
    pub(crate) kind: PageViewMutKind<'a, K, V>,
}

pub(crate) enum PageViewMutKind<'a, K, V> {
    Interior(InteriorPageViewMut<'a, K>),
    Leaf(LeafPageViewMut<'a, K, V>),
}

impl<'a, K, V> PageViewMut<'a, K, V> {
    pub(crate) async fn init_root_interior(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<InteriorPageViewMut<'a, K>> {
        Self::init_interior_inner(data, Flags::IS_ROOT).await
    }

    pub(crate) async fn init_interior(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<InteriorPageViewMut<'a, K>> {
        Self::init_interior_inner(data, Flags::empty()).await
    }

    pub(crate) async fn init_interior_inner(
        data: &'a mut [u8; PAGE_DATA_SIZE],
        flags: Flags,
    ) -> nsql_serde::Result<InteriorPageViewMut<'a, K>> {
        data.fill(0);
        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &PageHeader::new(flags));
        InteriorPageViewMut::<K>::init(data).await
    }

    pub(crate) async fn init_root_leaf(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        Self::init_leaf_inner(data, Flags::IS_LEAF | Flags::IS_ROOT).await
    }

    pub(crate) async fn init_leaf(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        Self::init_leaf_inner(data, Flags::IS_LEAF).await
    }

    async fn init_leaf_inner(
        data: &'a mut [u8; PAGE_DATA_SIZE],
        flags: Flags,
    ) -> nsql_serde::Result<LeafPageViewMut<'a, K, V>> {
        data.fill(0);
        let (header_bytes, data) = data.split_array_mut();
        nsql_rkyv::serialize_into_buf(header_bytes, &PageHeader::new(flags));

        LeafPageViewMut::<K, V>::init(data).await
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageViewMut<'a, K, V>> {
        let (header_bytes, data) = data.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(header_bytes) };
        let kind = if header.flags.contains(Flags::IS_LEAF) {
            LeafPageViewMut::<'a, K, V>::create(data).await.map(PageViewMutKind::Leaf)
        } else {
            InteriorPageViewMut::<'a, K>::create(data).await.map(PageViewMutKind::Interior)
        }?;

        Ok(PageViewMut { header, kind })
    }
}
