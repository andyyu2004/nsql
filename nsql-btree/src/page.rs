mod interior;
mod key_value_pair;
mod leaf;
mod node;
mod slotted;

use std::fmt;
use std::pin::Pin;

pub(crate) use key_value_pair::{ArchivedKeyValuePair, KeyValuePair};
pub(crate) use node::{Node, NodeMut};
use nsql_pager::PAGE_DATA_SIZE;
use rkyv::Archive;

pub(crate) use self::interior::{InteriorPageView, InteriorPageViewMut};
pub(crate) use self::leaf::{LeafPageView, LeafPageViewMut};

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct Flags: u8 {
        const IS_ROOT = 1 << 0;
        const IS_LEAF = 1 << 1;
    }
}

impl Archive for Flags {
    type Archived = Flags;
    type Resolver = ();

    unsafe fn resolve(&self, _: usize, (): Self::Resolver, out: *mut Self::Archived) {
        out.write(*self);
    }
}

impl<S: rkyv::ser::Serializer + ?Sized> rkyv::Serialize<S> for Flags {
    fn serialize(
        &self,
        _serializer: &mut S,
    ) -> Result<Self::Resolver, <S as rkyv::Fallible>::Error> {
        Ok(())
    }
}

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
    K: Archive + fmt::Debug,
    K::Archived: fmt::Debug + Ord,
    V: Archive + fmt::Debug,
    V::Archived: fmt::Debug,
{
    pub(crate) async unsafe fn create(
        data: &'a [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageView<'a, K, V>> {
        let (header_bytes, data) = data.split_array_ref();
        let header = unsafe { nsql_rkyv::archived_root::<PageHeader>(header_bytes) };
        if header.flags.contains(Flags::IS_LEAF) {
            LeafPageView::create(data).map(Self::Leaf)
        } else {
            InteriorPageView::create(data).map(Self::Interior)
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
    pub(crate) async unsafe fn create(
        data: &'a mut [u8; PAGE_DATA_SIZE],
    ) -> nsql_serde::Result<PageViewMut<'a, K, V>> {
        let (header_bytes, data) = data.split_array_mut();
        let header = unsafe { nsql_rkyv::archived_root_mut::<PageHeader>(header_bytes) };
        let kind = if header.flags.contains(Flags::IS_LEAF) {
            LeafPageViewMut::<'a, K, V>::create(data).map(PageViewMutKind::Leaf)
        } else {
            InteriorPageViewMut::<'a, K>::create(data).map(PageViewMutKind::Interior)
        }?;

        Ok(PageViewMut { header, kind })
    }
}
