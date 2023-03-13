mod interior;
mod leaf;
mod slotted;

use std::fmt;

use nsql_pager::PAGE_DATA_SIZE;
use nsql_serde::{Deserialize, DeserializeSkip, Serialize, SerializeSized};

pub(crate) use self::interior::{InteriorPageView, InteriorPageViewMut};
pub(crate) use self::leaf::{LeafPageView, LeafPageViewMut};
use crate::node::Flags;

macro_rules! sizeof {
    ($ty:ty) => {
        mem::size_of::<$ty>() as u16
    };
}

pub(crate) use sizeof;

#[derive(Debug)]
pub(crate) struct PageFull;

#[derive(Debug, SerializeSized, Deserialize)]
pub(crate) struct PageHeader {
    pub(crate) flags: Flags,
    filler: [u8; 3],
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
        let header = PageHeader::deserialize(&mut &data[..]).await?;
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageView::create(&data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(Self::Leaf)
        } else {
            InteriorPageView::create(&data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(Self::Interior)
        }
    }
}

pub(crate) struct PageViewMut<'a, K, V> {
    pub(crate) header: PageHeader,
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
        data.fill(0);
        PageHeader { flags: Flags::IS_ROOT, filler: [0; 3] }.serialize_into(data).await?;
        InteriorPageViewMut::<K>::init(&mut data[PageHeader::SERIALIZED_SIZE as usize..]).await
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
        PageHeader { flags, filler: [0; 3] }.serialize_into(data).await?;

        LeafPageViewMut::<K, V>::init(&mut data[PageHeader::SERIALIZED_SIZE as usize..]).await
    }

    pub(crate) async unsafe fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageViewMut<'a, K, V>> {
        let header = PageHeader::deserialize(&mut &data[..]).await?;
        let kind = if header.flags.contains(Flags::IS_LEAF) {
            LeafPageViewMut::<'a, K, V>::create(&mut data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(PageViewMutKind::Leaf)
        } else {
            InteriorPageViewMut::<'a, K>::create(&mut data[PageHeader::SERIALIZED_SIZE as usize..])
                .await
                .map(PageViewMutKind::Interior)
        }?;

        Ok(PageViewMut { header, kind })
    }
}
