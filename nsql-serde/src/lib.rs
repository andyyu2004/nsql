#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(min_specialization)]

use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrayvec::ArrayVec;
use nsql_arena::{Arena, Idx, RawIdx};
pub use nsql_serde_derive::{Deserialize, Serialize};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncWrite};
pub use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub trait Serializer<'s>: AsyncWrite + Send + Unpin {
    fn write_str(self, s: &'s str) -> Pin<Box<dyn Future<Output = io::Result<()>> + 's>>
    where
        Self: Sized + Unpin;
}

impl<'s, W: AsyncWrite + Send + Unpin + 's> Serializer<'s> for W {
    #[inline]
    fn write_str(mut self, s: &'s str) -> Pin<Box<dyn Future<Output = io::Result<()>> + 's>>
    where
        Self: Sized + Unpin,
    {
        Box::pin(async move {
            self.write_u32(s.len() as u32).await?;
            self.write_all(s.as_bytes()).await
        })
    }
}

pub trait Deserializer<'de>: AsyncRead + Send + Unpin {
    fn read_str(self) -> Pin<Box<dyn Future<Output = io::Result<SmolStr>> + 'de>>
    where
        Self: Sized + Unpin;
}

impl<'de, D: AsyncRead + Send + Unpin + 'de> Deserializer<'de> for D {
    #[inline]
    fn read_str(mut self) -> Pin<Box<dyn Future<Output = io::Result<SmolStr>> + 'de>>
    where
        Self: Sized + Unpin,
    {
        Box::pin(async move {
            let len = self.read_u32().await? as usize;
            let mut buf = vec![0; len];
            self.read_exact(&mut buf).await?;
            let s = std::str::from_utf8(&buf)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            Ok(SmolStr::from(s))
        })
    }
}

pub trait Serialize {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error>;

    #[inline]
    async fn serialized_size(&self) -> Result<usize, io::Error> {
        let mut counter = Counter::default();
        self.serialize(&mut counter).await?;
        Ok(counter.size)
    }
}

impl<S: Serialize + ?Sized> Serialize for Box<S> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        (**self).serialize(ser).await
    }
}

impl<S: Serialize> Serialize for [S] {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        // NOTE: no length prefixed unlike the Vec<S> impl
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

impl<S: Serialize> Serialize for Arc<S> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        (**self).serialize(ser).await
    }
}

impl<S: Serialize> Serialize for Vec<S> {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_u32(self.len() as u32).await?;
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

pub trait SerializeWith: Sized {
    type Context<'a>;

    async fn serialize_with(
        &self,
        ctx: &Self::Context<'_>,
        ser: &mut dyn Serializer<'_>,
    ) -> Result<(), io::Error>;
}

impl<S: Serialize> SerializeWith for S {
    type Context<'a> = ();

    async fn serialize_with(
        &self,
        _ctx: &Self::Context<'_>,
        ser: &mut dyn Serializer<'_>,
    ) -> Result<(), io::Error> {
        self.serialize(ser).await
    }
}

/// deserialization trait with context (analogous to serde::DeserializeSeed)
pub trait DeserializeWith: Sized {
    type Context<'a>;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, io::Error>;
}

impl<D: Deserialize> DeserializeWith for D {
    type Context<'a> = ();

    #[inline]
    async fn deserialize_with(
        _ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, io::Error> {
        D::deserialize(de).await
    }
}

pub trait Deserialize: Sized {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error>;
}

impl<D: Deserialize> Deserialize for Vec<D> {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let len = de.read_u32().await? as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(D::deserialize(de).await?);
        }
        Ok(items)
    }
}

#[derive(Debug, Default)]
struct Counter {
    size: usize,
}

impl AsyncWrite for Counter {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        this.size += buf.len();
        Poll::Ready(Ok(buf.len()))
    }

    #[inline]
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    #[inline]
    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

macro_rules! impl_serialize_primitive {
    ($method:ident: $ty:ty) => {
        impl Serialize for $ty {
            #[inline]
            async fn serialize(&self, buf: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
                buf.$method(*self).await?;
                Ok(())
            }
        }
    };
}

impl_serialize_primitive!(write_u8: u8);
impl_serialize_primitive!(write_u16: u16);
impl_serialize_primitive!(write_u32: u32);
impl_serialize_primitive!(write_u64: u64);

// spec disabled for now for async fn in trait
// FIXME https://github.com/rust-lang/rust/pull/108551
// impl<const N: usize> Serialize for [u8; N] {
//     #[inline]
//     async fn serialize(&self, buf: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
//         buf.write_all(self).await?;
//         Ok(())
//     }
// }

impl<const N: usize, T: Serialize> Serialize for [T; N] {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

impl<const N: usize, T: Serialize> Serialize for ArrayVec<T, N> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

impl Serialize for SmolStr {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_str(self).await
    }
}

impl Deserialize for SmolStr {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        de.read_str().await
    }
}

macro_rules! impl_deserialize {
    ($method:ident: $ty:ty) => {
        impl Deserialize for $ty {
            #[inline]
            async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<$ty, io::Error> {
                de.$method().await
            }
        }
    };
}

impl_deserialize!(read_u8: u8);
impl_deserialize!(read_u16: u16);
impl_deserialize!(read_u32: u32);
impl_deserialize!(read_u64: u64);

impl Serialize for bool {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_u8(*self as u8).await?;
        Ok(())
    }
}

impl Deserialize for bool {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        Ok(de.read_u8().await? != 0)
    }
}

impl Serialize for NonZeroU32 {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_u32(self.get()).await?;
        Ok(())
    }
}

impl Deserialize for NonZeroU32 {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        Ok(NonZeroU32::new(de.read_u32().await?).unwrap())
    }
}

// FIXME when specialization is fixed
// impl<const N: usize> Deserialize for [u8; N] {
//     #[inline]
//     async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
//         let mut buf = [0; N];
//         de.read_exact(&mut buf).await?;
//         Ok(buf)
//     }
// }

impl<const N: usize, T: Deserialize> Deserialize for [T; N] {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let xs = ArrayVec::<T, N>::deserialize(de).await?;
        // SAFETY: we just initialized each item in the array
        Ok(unsafe { xs.into_inner_unchecked() })
    }
}

impl<const N: usize, T: Deserialize> Deserialize for ArrayVec<T, N> {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let mut xs = ArrayVec::new();
        for _ in 0..N {
            xs.push(T::deserialize(de).await?);
        }
        Ok(xs)
    }
}

impl Serialize for Decimal {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_all(&self.serialize()).await?;
        Ok(())
    }
}

impl Deserialize for Decimal {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let mut buf = [0; 16];
        de.read_exact(&mut buf).await?;
        Ok(Decimal::deserialize(buf))
    }
}

impl<T> Serialize for Idx<T> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        self.into_raw().serialize(ser).await
    }
}

impl Serialize for RawIdx {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_u32(u32::from(*self)).await?;
        Ok(())
    }
}

impl<T> Deserialize for Idx<T> {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        Ok(Idx::from_raw(RawIdx::deserialize(de).await?))
    }
}

impl Deserialize for RawIdx {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        Ok(RawIdx::from(de.read_u32().await?))
    }
}

/// A type that has a value that represents an invalid state that corresponds to `Option::None`
pub trait Invalid {
    fn invalid() -> Self;
}

impl<T> Serialize for Option<T>
where
    T: Serialize + Invalid,
{
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        match self {
            Some(it) => it.serialize(ser).await,
            None => T::invalid().serialize(ser).await,
        }
    }
}

impl<T> Deserialize for Option<T>
where
    T: Deserialize + Invalid + PartialEq,
{
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let val = T::deserialize(de).await?;
        if val == T::invalid() { Ok(None) } else { Ok(Some(val)) }
    }
}

impl<T> Serialize for PhantomData<T> {
    #[inline]
    async fn serialize(&self, _: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        Ok(())
    }
}

impl<T> Deserialize for PhantomData<T> {
    #[inline]
    async fn deserialize(_: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        Ok(PhantomData)
    }
}

impl<T: Serialize> Serialize for Arena<T> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        ser.write_u32(self.len() as u32).await?;
        for (_idx, v) in self.iter() {
            v.serialize(ser).await?;
        }
        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Arena<T> {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let len = de.read_u32().await? as usize;
        let mut arena = Arena::with_capacity(len);
        for _ in 0..len {
            arena.alloc(T::deserialize(de).await?);
        }
        Ok(arena)
    }
}

impl<T: Serialize, U: Serialize> Serialize for (T, U) {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        self.0.serialize(ser).await?;
        self.1.serialize(ser).await?;
        Ok(())
    }
}

impl<T: Deserialize, U: Deserialize> Deserialize for (T, U) {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, io::Error> {
        let a = T::deserialize(de).await?;
        let b = U::deserialize(de).await?;
        Ok((a, b))
    }
}

pub trait VecSerExt<T> {
    /// Returns a wrapper around the vector that implements `Serialize` that will not prefix the length
    /// The length must be serialized elsewhere to be able to deserialize this type
    fn noninline_len(&self) -> NonInlineLengthVec<'_, T>;
}

pub trait VecDeExt<T> {
    async fn deserialize_noninline_len(
        de: &mut dyn Deserializer<'_>,
        len: usize,
    ) -> Result<Self, io::Error>
    where
        Self: Sized;
}

impl<T> VecSerExt<T> for Vec<T> {
    fn noninline_len(&self) -> NonInlineLengthVec<'_, T> {
        NonInlineLengthVec { data: self }
    }
}

impl<T: Deserialize> VecDeExt<T> for Vec<T> {
    async fn deserialize_noninline_len(
        de: &mut dyn Deserializer<'_>,
        len: usize,
    ) -> Result<Self, io::Error>
    where
        Self: Sized,
    {
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(T::deserialize(de).await?);
        }
        Ok(vec)
    }
}

#[derive(Debug)]
pub struct NonInlineLengthVec<'a, T> {
    data: &'a [T],
}

impl<T: Serialize> Serialize for NonInlineLengthVec<'_, T> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), io::Error> {
        for v in self.data {
            v.serialize(ser).await?;
        }
        Ok(())
    }
}
