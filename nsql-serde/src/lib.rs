#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(never_type)]

use std::io;

pub use bytes::{Buf, BufMut};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

pub trait Serializer: AsyncWrite + Unpin {}

impl<W: AsyncWrite + Unpin> Serializer for W {}

pub trait Deserializer: AsyncRead + Unpin {}

impl<R: AsyncRead + Unpin> Deserializer for R {}

pub trait Serialize {
    type Error: From<io::Error> = io::Error;
    async fn serialize(&self, writer: &mut dyn Serializer) -> Result<(), Self::Error>;
}

pub trait Deserialize: Sized {
    type Error: From<io::Error> = io::Error;
    async fn deserialize(reader: &mut dyn Deserializer) -> Result<Self, Self::Error>;
}

impl<S: Serialize> Serialize for Vec<S> {
    type Error = S::Error;

    async fn serialize(&self, writer: &mut dyn Serializer) -> Result<(), Self::Error> {
        writer.write_u32(self.len() as u32).await?;
        for item in self {
            item.serialize(writer).await?;
        }
        Ok(())
    }
}

impl<D: Deserialize> Deserialize for Vec<D> {
    type Error = D::Error;

    async fn deserialize(reader: &mut dyn Deserializer) -> Result<Self, Self::Error> {
        let len = reader.read_u32().await? as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(D::deserialize(reader).await?);
        }
        Ok(items)
    }
}

pub trait SerializeSync {
    fn serialize_sync(&self, buf: &mut dyn BufMut);
}

pub trait DeserializeSync: Sized {
    fn deserialize_sync(buf: &mut dyn Buf) -> Self;
}

// FIXME these blanket impls aren't the most efficient as they read the entire payload into memory and then copy it over
impl<S: SerializeSync> Serialize for S {
    type Error = io::Error;

    async fn serialize(&self, writer: &mut dyn Serializer) -> Result<(), Self::Error> {
        let mut buf = bytes::BytesMut::new();
        self.serialize_sync(&mut buf);
        writer.write_all(&buf).await?;
        Ok(())
    }
}

impl<D: DeserializeSync> Deserialize for D {
    type Error = io::Error;

    async fn deserialize(mut reader: &mut dyn Deserializer) -> Result<Self, Self::Error> {
        let mut buf = bytes::BytesMut::new();
        (&mut reader).read_buf(&mut buf).await?;
        Ok(Self::deserialize_sync(&mut buf))
    }
}
