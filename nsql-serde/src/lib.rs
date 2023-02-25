#![deny(rust_2018_idioms)]
#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(never_type)]

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

pub use bytes::{Buf, BufMut};
pub use nsql_serde_derive::{Deserialize, DeserializeSync, Serialize, SerializeSync};
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
    type Error: From<io::Error> = io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error>;
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
    type Context;
    type Error: From<io::Error> = io::Error;

    async fn deserialize_with(
        ctx: &Self::Context,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error>;
}

impl<D: Deserialize> DeserializeWith for D {
    type Context = ();
    type Error = D::Error;

    async fn deserialize_with(
        _ctx: &Self::Context,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error> {
        D::deserialize(de).await
    }
}

pub trait Deserialize: Sized {
    type Error: From<io::Error> = io::Error;

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

pub trait SerializeSync {
    fn serialize_sync(&self, buf: &mut dyn BufMut);
}

impl SerializeSync for u32 {
    #[inline]
    fn serialize_sync(&self, buf: &mut dyn BufMut) {
        buf.put_u32(*self);
    }
}

impl<const N: usize> SerializeSync for [u8; N] {
    #[inline]
    fn serialize_sync(&self, buf: &mut dyn BufMut) {
        buf.put_slice(self);
    }
}

pub trait DeserializeSync: Sized {
    fn deserialize_sync(buf: &mut dyn Buf) -> Self;
}

impl DeserializeSync for u32 {
    #[inline]
    fn deserialize_sync(buf: &mut dyn Buf) -> u32 {
        buf.get_u32()
    }
}

impl<const N: usize> DeserializeSync for [u8; N] {
    #[inline]
    fn deserialize_sync(buf: &mut dyn Buf) -> [u8; N] {
        let mut bytes = [0; N];
        buf.copy_to_slice(&mut bytes);
        bytes
    }
}

// FIXME these blanket impls aren't the most efficient as they read the entire payload into memory and then copy it over
impl<S: SerializeSync> Serialize for S {
    type Error = io::Error;

    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        let mut buf = bytes::BytesMut::new();
        self.serialize_sync(&mut buf);
        ser.write_all(&buf).await?;
        Ok(())
    }
}

impl<D: DeserializeSync> Deserialize for D {
    type Error = io::Error;

    async fn deserialize(mut de: &mut dyn Deserializer<'_>) -> Result<Self, Self::Error> {
        let mut buf = bytes::BytesMut::new();
        (&mut de).read_buf(&mut buf).await?;
        Ok(Self::deserialize_sync(&mut buf))
    }
}
