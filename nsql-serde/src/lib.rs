#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(min_specialization)]

use core::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrayvec::ArrayVec;
use nsql_arena::{Idx, RawIdx};
pub use nsql_serde_derive::{Deserialize, Serialize};
use rust_decimal::Decimal;
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncWrite};
pub use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub trait Serializer<'s>: AsyncWrite + Unpin {
    fn write_str(self, s: &'s str) -> Pin<Box<dyn Future<Output = io::Result<()>> + 's>>
    where
        Self: Sized + Unpin;
}

impl<'s, W: AsyncWrite + Unpin + 's> Serializer<'s> for W {
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

pub trait Deserializer<'de>: AsyncRead + Unpin {
    fn read_str(self) -> Pin<Box<dyn Future<Output = io::Result<SmolStr>> + 'de>>
    where
        Self: Sized + Unpin;
}

impl<'de, D: AsyncRead + Unpin + 'de> Deserializer<'de> for D {
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
    type Error: From<io::Error> + fmt::Debug = io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error>;

    async fn serialized_size(&self) -> Result<usize, Self::Error> {
        let mut counter = Counter::default();
        self.serialize(&mut counter).await?;
        Ok(counter.size)
    }
}

impl<S: Serialize + ?Sized> Serialize for Box<S> {
    type Error = S::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        (**self).serialize(ser).await
    }
}

impl<S: Serialize> Serialize for [S] {
    type Error = S::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        // NOTE: no length prefixed unlike the Vec<S> impl
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

impl<S: Serialize> Serialize for Arc<S> {
    type Error = S::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        (**self).serialize(ser).await
    }
}

impl<S: Serialize> Serialize for Vec<S> {
    type Error = S::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_u32(self.len() as u32).await?;
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

/// deserialization trait with context (analogous to serde::DeserializeSeed)
pub trait DeserializeWith: Sized {
    type Context<'a>;
    type Error: From<io::Error> = io::Error;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error>;
}

impl<D: Deserialize> DeserializeWith for D {
    type Context<'a> = ();
    type Error = D::Error;

    async fn deserialize_with(
        _ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error> {
        D::deserialize(de).await
    }
}

pub trait Deserialize: Sized {
    type Error: From<io::Error> + fmt::Debug = io::Error;

    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error>;
}

impl<D: Deserialize> Deserialize for Vec<D> {
    type Error = D::Error;

    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
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
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        this.size += buf.len();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }
}

macro_rules! impl_serialize {
    ($method:ident: $ty:ty) => {
        impl Serialize for $ty {
            #[inline]
            async fn serialize(&self, buf: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
                buf.$method(*self).await?;
                Ok(())
            }
        }
    };
}

impl_serialize!(write_u8: u8);
impl_serialize!(write_u16: u16);
impl_serialize!(write_u32: u32);
impl_serialize!(write_u64: u64);

impl<const N: usize> Serialize for [u8; N] {
    #[inline]
    async fn serialize(&self, buf: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        buf.write_all(self).await?;
        Ok(())
    }
}

impl<const N: usize, T: Serialize> Serialize for [T; N] {
    type Error = T::Error;

    #[inline]
    default async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        for item in self {
            item.serialize(ser).await?;
        }
        Ok(())
    }
}

impl Serialize for SmolStr {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_str(self).await
    }
}

impl Deserialize for SmolStr {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        de.read_str().await
    }
}

macro_rules! impl_deserialize {
    ($method:ident: $ty:ty) => {
        impl Deserialize for $ty {
            #[inline]
            async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<$ty, Self::Error> {
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
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_u8(*self as u8).await?;
        Ok(())
    }
}

impl Deserialize for bool {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        Ok(de.read_u8().await? != 0)
    }
}

impl<const N: usize> Deserialize for [u8; N] {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let mut buf = [0; N];
        de.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

impl<const N: usize, T: Deserialize> Deserialize for [T; N] {
    type Error = T::Error;

    default async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let mut xs = ArrayVec::new();
        for _ in 0..N {
            xs.push(T::deserialize(de).await?);
        }
        // SAFETY: we just initialized each item in the array
        Ok(unsafe { xs.into_inner_unchecked() })
    }
}

impl Serialize for Decimal {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_all(&self.serialize()).await?;
        Ok(())
    }
}

impl Deserialize for Decimal {
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let mut buf = [0; 16];
        de.read_exact(&mut buf).await?;
        Ok(Decimal::deserialize(buf))
    }
}

impl<T> Serialize for Idx<T> {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        self.into_raw().serialize(ser).await
    }
}

impl Serialize for RawIdx {
    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        ser.write_u32(u32::from(*self)).await?;
        Ok(())
    }
}

impl<T> Deserialize for Idx<T> {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        Ok(Idx::from_raw(RawIdx::deserialize(de).await?))
    }
}

impl Deserialize for RawIdx {
    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
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
    type Error = T::Error;

    #[inline]
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
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
    type Error = T::Error;

    #[inline]
    async fn deserialize(de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let val = T::deserialize(de).await?;
        if val == T::invalid() { Ok(None) } else { Ok(Some(val)) }
    }
}
