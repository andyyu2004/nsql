use std::sync::Arc;
use std::{io, mem};

use nsql_pager::{Pager, PAGE_SIZE};
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, DeserializeWith, Deserializer, Serialize, Serializer,
};
use nsql_transaction::Transaction;
use nsql_util::static_assert_eq;

use crate::tuple::{Tuple, TupleDeserializationContext};
use crate::Result;

pub struct TableStorage {
    pager: Arc<dyn Pager>,
}

#[derive(Debug, PartialEq)]
struct HeapTuplePage {
    header: HeapTuplePageHeader,
    tuple_offsets: Vec<TupleOffset>,
    tuples: Vec<HeapTuple>,
}

impl Default for HeapTuplePage {
    fn default() -> Self {
        Self {
            header: HeapTuplePageHeader {
                free_start: TUPLE_PAGE_HEADER_SIZE,
                free_end: PAGE_SIZE as u16,
            },
            tuple_offsets: Default::default(),
            tuples: Default::default(),
        }
    }
}

impl Serialize for HeapTuplePage {
    async fn serialize(&self, ser: &mut dyn Serializer<'_>) -> Result<(), Self::Error> {
        self.header.serialize(ser).await?;

        ser.write_u16(self.tuple_offsets.len() as u16).await?;
        for offset in &self.tuple_offsets {
            offset.serialize(ser).await?;
        }

        // put the padding for free space
        for _ in 0..self.header.free_space() {
            ser.write_u8(0).await?;
        }

        // don't use the vec impl for serialize, as we don't want the length prefix
        for tuple in &self.tuples {
            tuple.serialize(ser).await?;
        }

        Ok(())
    }
}

impl DeserializeWith for HeapTuplePage {
    type Context<'a> = TupleDeserializationContext<'a>;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error> {
        let header = HeapTuplePageHeader::deserialize(de).await?;
        let n = de.read_u16().await? as usize;

        let mut item_offsets = Vec::with_capacity(n);
        for _ in 0..n {
            item_offsets.push(TupleOffset::deserialize(de).await?);
        }

        // read the padding for free space
        for _ in 0..header.free_space() {
            de.read_u8().await?;
        }

        let mut items = Vec::with_capacity(n);
        for offset in &item_offsets {
            let mut tuple_de = de.take(offset.length as u64);
            items.push(HeapTuple::deserialize_with(ctx, &mut tuple_de).await?);
        }

        Ok(Self { header, tuple_offsets: item_offsets, tuples: items })
    }

    type Error = io::Error;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TupleOffset {
    offset: u16,
    length: u16,
}

const TUPLE_PAGE_HEADER_SIZE: u16 = mem::size_of::<HeapTuplePageHeader>() as u16;
static_assert_eq!(TUPLE_PAGE_HEADER_SIZE, 4);

const TUPLE_HEADER_SIZE: u16 = mem::size_of::<HeapTupleHeader>() as u16;
static_assert_eq!(TUPLE_HEADER_SIZE, 0);

const ITEM_OFFSET_SIZE: u16 = mem::size_of::<TupleOffset>() as u16;
static_assert_eq!(ITEM_OFFSET_SIZE, 4);

impl HeapTuplePage {
    async fn insert(&mut self, tuple: Tuple) -> Result<(), io::Error> {
        let length = tuple.serialized_size().await? as u16;
        let heap_tuple = HeapTuple { header: HeapTupleHeader {}, tuple };
        if self.header.free_space() < length + TUPLE_HEADER_SIZE {
            todo!("page is full")
        }

        self.header.free_end -= length;
        self.header.free_start += TUPLE_HEADER_SIZE;
        debug_assert!(self.header.free_start <= self.header.free_end);

        let offset = TupleOffset { offset: self.header.free_end, length };
        self.tuple_offsets.push(offset);
        self.tuples.push(heap_tuple);

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct HeapTuplePageHeader {
    free_start: u16,
    free_end: u16,
}

impl HeapTuplePageHeader {
    fn free_space(&self) -> u16 {
        self.free_end - self.free_start
    }
}

#[derive(Debug, PartialEq, Serialize)]
struct HeapTuple {
    header: HeapTupleHeader,
    tuple: Tuple,
}

impl DeserializeWith for HeapTuple {
    type Context<'a> = TupleDeserializationContext<'a>;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer<'_>,
    ) -> Result<Self, Self::Error> {
        let header = HeapTupleHeader::deserialize(de).await?;
        let tuple = Tuple::deserialize_with(ctx, de).await?;
        Ok(Self { header, tuple })
    }
}

impl HeapTuple {}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct HeapTupleHeader {}

impl TableStorage {
    pub fn new(pager: Arc<dyn Pager>) -> Self {
        Self { pager }
    }

    pub async fn append(&self, _tx: &Transaction, _tuple: Tuple) -> Result<()> {
        todo!()
    }

    pub async fn scan(_tx: &Transaction) -> Vec<Tuple> {
        todo!()
    }
}

#[cfg(test)]
mod tests;
