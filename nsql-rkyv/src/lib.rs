//! utility functions for using rkyv
//! we support only `archive(as = "Self")` types

#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

use rkyv::ser::serializers::AllocSerializer;
use rkyv::Serialize;

#[inline]
pub fn to_bytes<T>(value: &T) -> rkyv::AlignedVec
where
    T: Serialize<AllocSerializer<4096>>,
{
    rkyv::to_bytes(value).expect("rkyv serialization failed")
}

/// convert `T` to `T::Archived`
// limited to `Copy` types
#[inline]
pub fn to_archive<T>(value: T) -> T::Archived
where
    T: Serialize<AllocSerializer<4096>> + Copy,
    T::Archived: Copy,
{
    let bytes = to_bytes(&value);
    unsafe { *rkyv::archived_root::<T>(&bytes) }
}
