//! utility functions for using rkyv
//! we support only `archive(as = "Self")` types

#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

use std::mem;
use std::pin::Pin;

use rkyv::ser::serializers::{AllocSerializer, BufferSerializer};
use rkyv::ser::Serializer;
use rkyv::{Archive, Serialize};

#[inline]
pub fn serialize_into_buf<'a, T>(buf: &'a mut [u8; mem::size_of::<T::Archived>()], value: &T)
where
    T: Serialize<BufferSerializer<&'a mut [u8; mem::size_of::<T::Archived>()]>>,
{
    let n = buf.len();
    let mut serializer = BufferSerializer::new(buf);
    serializer.serialize_value(value).unwrap_or_else(|err| {
        panic!(
            "should not fail as the buffer is large enough: {err} ({}, buf.len() = {n})",
            std::any::type_name::<T::Archived>(),
        )
    });
}

/// # Safety
/// refer to [`rkyv::archived_root`]
pub unsafe fn archived_root<T: Archive>(
    bytes: &[u8; mem::size_of::<T::Archived>()],
) -> &T::Archived {
    rkyv::archived_root::<T>(bytes)
}

/// # Safety
/// refer to [`rkyv::archived_root_mut`]
pub unsafe fn archived_root_mut<T: Archive>(
    bytes: &mut [u8; mem::size_of::<T::Archived>()],
) -> Pin<&mut T::Archived> {
    rkyv::archived_root_mut::<T>(Pin::new(bytes))
}

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

#[cfg(test)]
mod tests {

    use std::mem;

    use rkyv::{Archive, Serialize};

    use crate::{serialize_into_buf, to_bytes};

    #[test]
    fn test_serialize_into_buf() {
        let mut buf = [0u8; 4];
        let value = 0x12345678u32;
        serialize_into_buf(&mut buf, &value);
        assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_serialize_into_buf_2() {
        #[derive(Debug, Archive, Serialize, Default)]
        struct S {
            a: u32,
            b: u32,
            c: [u8; 4],
        }
        let mut buf = [0u8; mem::size_of::<<S as Archive>::Archived>()];
        serialize_into_buf(&mut buf, &S::default());
    }

    #[test]
    fn test_to_bytes() {
        let value = 0x12345678u32;
        let bytes = to_bytes(&value);
        assert_eq!(bytes.as_slice(), [0x12, 0x34, 0x56, 0x78]);
    }
}
