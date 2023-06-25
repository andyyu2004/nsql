//! utility functions for using rkyv

#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

use std::mem;
use std::pin::Pin;

use rkyv::ser::serializers::{
    AlignedSerializer, AllocScratch, BufferSerializer, CompositeSerializer,
};
use rkyv::ser::Serializer as _;
use rkyv::{AlignedVec, Archive, ArchiveUnsized, Deserialize, Serialize, SerializeUnsized};

pub type DefaultSerializer =
    CompositeSerializer<AlignedSerializer<AlignedVec>, AllocScratch, rkyv::Infallible>;

pub type DefaultDeserializer = rkyv::Infallible;

#[inline]
pub fn deserialize<T: Archive>(archived: &T::Archived) -> T
where
    T::Archived: Deserialize<T, DefaultDeserializer>,
{
    archived.deserialize(&mut rkyv::Infallible).unwrap()
}

/// # Safety `raw` must be a valid archived value of type `T`
#[inline]
pub unsafe fn deserialize_raw<T: Archive>(raw: &[u8]) -> T
where
    T::Archived: Deserialize<T, DefaultDeserializer>,
{
    let archived = rkyv::archived_root::<T>(raw);
    archived.deserialize(&mut rkyv::Infallible).unwrap()
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
    T: SerializeUnsized<DefaultSerializer> + ?Sized,
{
    let mut serializer = DefaultSerializer::default();
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
    serializer.into_serializer().into_inner()
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

/// Wrapper around `align_offset` specialized to archived types and represents a left offset rather than right.
/// i.e. the offset should be subtracted from the pointer to align it.
pub fn align_archived_ptr_offset<T: Archive>(ptr: *const u8) -> usize {
    let alignment = archived_align_of!(T);
    let align_offset = ptr.align_offset(alignment);
    if align_offset != 0 {
        debug_assert!(
            align_offset < alignment,
            "the offset required to meet alignment should be less than the alignment"
        );
        // We apply the alignment offset to make the slot aligned, but we can't shift the
        // pointer forward as we might be going into used space.
        // Therefore, we subtract an entire alignments worth of space first.
        alignment - align_offset
    } else {
        0
    }
}

#[macro_export]
macro_rules! archived_size_of {
    ($ty:ty) => {
        ::std::mem::size_of::<::rkyv::Archived<$ty>>() as u16
    };
}

#[macro_export]
macro_rules! archived_align_of {
    ($ty:ty) => {
        ::std::mem::align_of::<::rkyv::Archived<$ty>>()
    };
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
