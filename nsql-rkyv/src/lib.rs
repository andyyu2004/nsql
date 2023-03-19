//! utility functions for using rkyv

#![feature(generic_const_exprs, specialization, min_specialization, rustc_attrs)]
#![allow(incomplete_features)]

use std::mem;
use std::pin::Pin;

use rkyv::ser::serializers::{AlignedSerializer, AllocSerializer, BufferSerializer};
use rkyv::ser::Serializer as _;
use rkyv::{
    AlignedVec, Archive, ArchiveUnsized, Deserialize, Infallible, Serialize, SerializeUnsized,
};

pub type DefaultSerializer = AlignedSerializer<AlignedVec>;
pub type DefaultUnsizedSerializer = AllocSerializer<256>;

#[inline]
pub fn deserialize<A, T>(archived: &A) -> T
where
    A: Deserialize<T, Infallible> + ?Sized,
{
    archived.deserialize(&mut Infallible).unwrap()
}

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
/// The archived value must start at the beginning of the given buffer
/// Also refer to [`rkyv::archived_unsized_value`]
pub unsafe fn archived_unsized<T>(bytes: &[u8]) -> &T::Archived
where
    T: ArchiveUnsized + ?Sized,
{
    rkyv::archived_unsized_value::<T>(bytes, 0)
}

pub fn serialize_unsized<T>(value: &T) -> rkyv::AlignedVec
where
    T: SerializeUnsized<DefaultUnsizedSerializer> + ?Sized,
{
    let mut serializer = DefaultUnsizedSerializer::default();
    let _pos = serializer.serialize_unsized_value(value).expect("should have enough scratch space");
    serializer.into_serializer().into_inner()
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

/// # Safety
/// refer to [`rkyv::archived_value`]
pub unsafe fn archived_value<T>(bytes: &[u8], pos: usize) -> &T::Archived
where
    T: Archive,
{
    rkyv::archived_value::<T>(bytes, pos)
}

#[inline]
pub fn to_bytes<T>(value: &T) -> rkyv::AlignedVec
where
    T: Serialize<DefaultSerializer>,
{
    let mut serializer = DefaultSerializer::default();
    serializer.serialize_value(value).unwrap();
    serializer.into_inner()
}

/// convert `T` to `T::Archived`
// limited to `Copy` types
#[inline]
pub fn to_archive<T>(value: T) -> T::Archived
where
    T: Serialize<DefaultSerializer> + Copy,
    T::Archived: Copy,
{
    let bytes = to_bytes(&value);
    unsafe { *rkyv::archived_root::<T>(&bytes) }
}

#[cfg(test)]
mod tests {

    use std::mem;

    use rkyv::{Archive, Serialize};

    use crate::{serialize_into_buf, serialize_unsized, to_bytes};

    #[test]
    fn test_serialize_primitive_into_buf() {
        let mut buf = [0u8; 4];
        let value = 0x12345678u32;
        serialize_into_buf(&mut buf, &value);
        assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_serialize_struct_into_buf() {
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

    #[test]
    fn test_serialize_unsized() {
        let mut value = [0u8; 24];
        value.iter_mut().enumerate().for_each(|(i, v)| *v = i as u8);

        let bytes = serialize_unsized(&value[..]);
        // let archived_value = unsafe { archived_unsized::<[u8]>(&bytes[..]) };
        let archived_value = unsafe { rkyv::archived_unsized_root::<[u8]>(&bytes[..]) };

        assert_eq!(archived_value, value);
    }
}
