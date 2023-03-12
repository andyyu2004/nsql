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
pub fn archive<T>(value: &T) -> rkyv::AlignedVec
where
    T: Serialize<AllocSerializer<4096>>,
    T: Archive,
{
    to_bytes(value).expect("rkyv serialization failed")
}

/// # Safety
/// See [`rkyv::archived_root_mut`]
#[inline]
pub unsafe fn unarchive_root_mut<T>(
    bytes: Pin<&mut [u8; mem::size_of::<T::Archived>()]>,
) -> Pin<&mut T::Archived>
where
    T: Archive,
{
    archived_root_mut::<T>(bytes)
}

// /// # Safety
// /// See [`rkyv::archived_value_mut`]
// #[inline]
// pub unsafe fn unarchive_mut<T>(
//     bytes: Pin<&mut [u8; mem::size_of::<T::Archived>()]>,
// ) -> Pin<&mut T::Archived>
// where
//     T: Archive,
//     Assert<{ mem::size_of::<T>() == mem::size_of::<T::Archived>() }>: True,
// {
//     archived_value_mut::<T>(bytes, mem::size_of::<T>())
// }
