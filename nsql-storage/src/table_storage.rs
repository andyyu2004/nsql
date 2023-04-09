mod fsm;

use std::sync::Arc;

use nsql_arena::{Arena, Idx};
use nsql_buffer::Pool;
use nsql_core::schema::Schema;
use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{
    AsyncReadExt, AsyncWriteExt, Deserialize, DeserializeWith, Deserializer, Serialize,
    SerializeSized, Serializer,
};
use nsql_transaction::Transaction;

use crate::tuple::{Tuple, TupleDeserializationContext};
use crate::Result;

pub struct TableStorage {
    pool: Arc<dyn Pool>,
    info: TableStorageInfo,
}

impl TableStorage {
    pub fn new(pool: Arc<dyn Pool>, info: TableStorageInfo) -> Self {
        Self { pool, info }
    }

    pub async fn append(&self, _tx: &Transaction, tuple: Tuple) -> nsql_serde::Result<()> {
        let size = tuple.serialized_size().await?;
        let idx = match self.find_free_space(size) {
            Some(_) => todo!(),
            None => self.pool.pager().alloc_page().await?,
        };
        let ctx = TupleDeserializationContext { schema: Arc::clone(&self.info.schema) };
        let handle = self.pool.load(idx).await?;
        let mut page = HeapTuplePage::deserialize_with(&ctx, &mut handle.page().read()).await?;
        let _slot = match page.insert_tuple(tuple).await? {
            Ok(slot) => slot,
            Err(HeapTuplePageFull) => panic!("there should be enough space as we checked fsm"),
        };
        Ok(())
    }

    pub async fn scan(_tx: &Transaction) -> Vec<Tuple> {
        todo!()
    }

    fn find_free_space(&self, _size: u16) -> Option<PageIndex> {
        todo!()
    }
}

pub struct TableStorageInfo {
    schema: Arc<Schema>,
    /// The index of the root page of the table if it has been allocated
    root_page_idx: Option<PageIndex>,
}

impl TableStorageInfo {
    #[inline]
    pub fn new(schema: Arc<Schema>, root_page_idx: Option<PageIndex>) -> Self {
        Self { schema, root_page_idx }
    }

    #[inline]
    pub fn create(schema: Arc<Schema>) -> Self {
        Self { schema, root_page_idx: None }
    }
}

#[derive(Debug, PartialEq)]
struct HeapTuplePage {
    header: HeapTuplePageHeader,
    slots: Arena<Slot>,
    tuples: Vec<HeapTuple>,
}

impl Default for HeapTuplePage {
    fn default() -> Self {
        Self {
            header: HeapTuplePageHeader {
                free_start: HeapTuplePageHeader::SERIALIZED_SIZE,
                free_end: PAGE_DATA_SIZE as u16,
            },
            slots: Default::default(),
            tuples: Default::default(),
        }
    }
}

impl Serialize for HeapTuplePage {
    async fn serialize(&self, ser: &mut dyn Serializer) -> nsql_serde::Result<()> {
        let ser = &mut ser.limit(PAGE_DATA_SIZE as u16);
        self.header.serialize(ser).await?;

        ser.write_u16(self.slots.len() as u16).await?;
        for offset in &self.slots {
            offset.serialize(ser).await?;
        }

        // put the padding for free space
        ser.fill(self.header.free_space()).await?;

        // don't use the vec impl for serialize, as we don't want the length prefix
        for tuple in &self.tuples {
            tuple.serialize(ser).await?;
        }

        Ok(())
    }
}

impl DeserializeWith for HeapTuplePage {
    type Context<'a> = TupleDeserializationContext;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer,
    ) -> nsql_serde::Result<Self> {
        let header = HeapTuplePageHeader::deserialize(de).await?;
        let n = de.read_u16().await? as usize;

        let mut slots = Arena::with_capacity(n);
        for _ in 0..n {
            slots.alloc(Slot::deserialize(de).await?);
        }

        // read the padding for free space
        de.skip_fill(header.free_space()).await?;

        let mut tuples = Vec::with_capacity(n);
        for offset in &slots {
            let mut tuple_de = de.take(offset.length as u64);
            tuples.push(HeapTuple::deserialize_with(ctx, &mut tuple_de).await?);
        }

        Ok(Self { header, slots, tuples })
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TupleId {
    page_idx: PageIndex,
    // FIXME this should be a u8
    slot_idx: Idx<Slot>,
}

#[derive(Debug, PartialEq, SerializeSized, Deserialize)]
struct Slot {
    /// The offset of the tuple from the start of the page
    offset: u16,
    /// The length of the tuple
    length: u16,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct HeapTuplePageFull;

impl HeapTuplePage {
    async fn insert_tuple(
        &mut self,
        tuple: Tuple,
    ) -> nsql_serde::Result<Result<Idx<Slot>, HeapTuplePageFull>> {
        let length = tuple.serialized_size().await? as u16;
        let heap_tuple = HeapTuple { header: HeapTupleHeader {}, tuple };
        // FIXME account for slot size too
        if self.header.free_space() < length + HeapTupleHeader::SERIALIZED_SIZE {
            return Ok(Err(HeapTuplePageFull));
        }

        self.header.free_end -= length;
        self.header.free_start += HeapTuplePageHeader::SERIALIZED_SIZE;
        debug_assert!(self.header.free_start <= self.header.free_end);

        let offset = Slot { offset: self.header.free_end, length };
        let slot_idx = self.slots.alloc(offset);
        self.tuples.push(heap_tuple);

        Ok(Ok(slot_idx))
    }
}

#[derive(Debug, PartialEq, SerializeSized, Deserialize)]
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

impl HeapTuple {
    /// The maximum size of a tuple that can be stored in a page
    pub const MAX_SIZE: u16 =
        PAGE_DATA_SIZE as u16 - HeapTuplePageHeader::SERIALIZED_SIZE - Slot::SERIALIZED_SIZE;
}

impl DeserializeWith for HeapTuple {
    type Context<'a> = TupleDeserializationContext;

    async fn deserialize_with(
        ctx: &Self::Context<'_>,
        de: &mut dyn Deserializer,
    ) -> nsql_serde::Result<Self> {
        let header = HeapTupleHeader::deserialize(de).await?;
        let tuple = Tuple::deserialize_with(ctx, de).await?;
        Ok(Self { header, tuple })
    }
}

impl HeapTuple {}

#[derive(Debug, PartialEq, SerializeSized, Deserialize)]
struct HeapTupleHeader {}

#[cfg(test)]
mod tests;
