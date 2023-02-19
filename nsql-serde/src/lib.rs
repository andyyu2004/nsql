#![allow(incomplete_features)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(never_type)]

use std::io;

use bytes::{Buf, BufMut};
use nsql_error::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub trait Serialize {
    type Error = io::Error;
    async fn serialize(&self, writer: &mut (dyn AsyncWrite + Unpin)) -> Result<(), Self::Error>;
}

pub trait Deserialize: Sized {
    type Error = io::Error;
    async fn deserialize(reader: &mut (dyn AsyncRead + Unpin)) -> Result<Self, Self::Error>;
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

    async fn serialize(&self, writer: &mut (dyn AsyncWrite + Unpin)) -> Result<(), Self::Error> {
        let mut buf = bytes::BytesMut::new();
        self.serialize_sync(&mut buf);
        writer.write_all(&buf).await?;
        Ok(())
    }
}

impl<D: DeserializeSync> Deserialize for D {
    type Error = io::Error;

    async fn deserialize(mut reader: &mut (dyn AsyncRead + Unpin)) -> Result<Self, Self::Error> {
        let mut buf = bytes::BytesMut::new();
        (&mut reader).read_buf(&mut buf).await?;
        Ok(Self::deserialize_sync(&mut buf))
    }
}
