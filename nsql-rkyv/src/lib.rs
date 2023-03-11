//! utility functions for using rkyv
//! we support only `archive(as = "Self")` types

#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

use std::mem;
use std::pin::Pin;

use rkyv::ser::serializers::AllocSerializer;
use rkyv::util::to_bytes;
use rkyv::{archived_root_mut, Archive, Serialize};

#[inline]
pub fn archive<T>(value: &T) -> [u8; mem::size_of::<T>()]
where
    T: Serialize<AllocSerializer<4096>>,
    T: Archive<Archived = T>,
{
    let d = to_bytes(value).unwrap();
    debug_assert_eq!(d.len(), mem::size_of::<T>());
    d.as_slice().try_into().unwrap()
}

/// # Safety
/// See [`rkyv::archived_root_mut`]
#[inline]
pub unsafe fn unarchive_mut<T>(bytes: Pin<&mut [u8; mem::size_of::<T>()]>) -> Pin<&mut T::Archived>
where
    T: Archive<Archived = T>,
{
    archived_root_mut::<T>(bytes)
}
