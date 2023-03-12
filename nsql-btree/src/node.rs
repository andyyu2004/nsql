use std::{io, mem};

use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, Deserializer, Serialize, SerializeSized, Serializer,
};

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct Flags: u8 {
        const IS_ROOT = 1 << 0;
        const IS_LEAF = 1 << 1;
        const VARIABLE_SIZE_KEYS = 1 << 2;
    }
}

impl Serialize for Flags {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        Ok(ser.write_u8(self.bits()).await?)
    }
}

impl Deserialize for Flags {
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let bits = de.read_u8().await?;
        match Self::from_bits(bits) {
            Some(flags) => Ok(flags),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid btree bitflags: {bits:#b}"),
            ))?,
        }
    }
}

impl SerializeSized for Flags {
    const SERIALIZED_SIZE: u16 = mem::size_of::<Self>() as u16;
}
