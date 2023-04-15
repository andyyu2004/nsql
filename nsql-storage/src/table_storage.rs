mod fsm;
mod heap;

use std::sync::Arc;

use nsql_arena::{Arena, Idx};
use nsql_buffer::Pool;
use nsql_core::schema::Schema;
use nsql_pager::{PageIndex, PAGE_DATA_SIZE};
use nsql_serde::{SerializeSized, StreamDeserialize, StreamSerializer};
use nsql_transaction::Transaction;

use crate::tuple::Tuple;
use crate::Result;

pub struct TableStorage {
    pool: Arc<dyn Pool>,
    info: TableStorageInfo,
}

impl TableStorage {
    pub fn new(pool: Arc<dyn Pool>, info: TableStorageInfo) -> Self {
        Self { pool, info }
    }

    pub async fn append(&self, _tx: &Transaction, _tuple: Tuple) -> nsql_serde::Result<()> {
        // let size = tuple.serialized_size().await?;
        // let idx = match self.find_free_space(size) {
        //     Some(_) => todo!(),
        //     None => self.pool.pager().alloc_page().await?,
        // };
        // let ctx = TupleDeserializationContext { schema: Arc::clone(&self.info.schema) };
        // let handle = self.pool.load(idx).await?;
        // let mut guard = handle.page().read();
        // todo!();
        // FIXME we should have a different trait that isn't async as this just reads one page
        // (opposed to the meta page)
        // let mut page = HeapTuplePage::deserialize_with(&ctx, &mut guard).await?;
        // let _slot = match page.insert_tuple(tuple).await? {
        //     Ok(slot) => slot,
        //     Err(HeapTuplePageFull) => panic!("there should be enough space as we checked fsm"),
        // };
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

#[derive(Debug, PartialEq, rkyv::Archive, rkyv::Serialize)]
struct TupleId {
    page_idx: PageIndex,
    // FIXME this should be a u8
    slot_idx: u8,
}

#[derive(Debug, PartialEq, SerializeSized, StreamDeserialize)]
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
        _tuple: Tuple,
    ) -> nsql_serde::Result<Result<Idx<Slot>, HeapTuplePageFull>> {
        todo!();
        // let length = tuple.serialized_size().await? as u16;
        // let heap_tuple = HeapTuple { header: HeapTupleHeader {}, tuple };
        // // FIXME account for slot size too
        // if self.header.free_space() < length + HeapTupleHeader::SERIALIZED_SIZE {
        //     return Ok(Err(HeapTuplePageFull));
        // }
        //
        // self.header.free_end -= length;
        // self.header.free_start += HeapTuplePageHeader::SERIALIZED_SIZE;
        // debug_assert!(self.header.free_start <= self.header.free_end);
        //
        // let offset = Slot { offset: self.header.free_end, length };
        // let slot_idx = self.slots.alloc(offset);
        // self.tuples.push(heap_tuple);
        //
        // Ok(Ok(slot_idx))
    }
}

#[derive(Debug, PartialEq, SerializeSized, StreamDeserialize)]
struct HeapTuplePageHeader {
    free_start: u16,
    free_end: u16,
}

impl HeapTuplePageHeader {
    fn free_space(&self) -> u16 {
        self.free_end - self.free_start
    }
}

#[derive(Debug, PartialEq, rkyv::Archive, rkyv::Serialize)]
struct HeapTuple {
    header: HeapTupleHeader,
    tuple: Tuple,
}

impl HeapTuple {
    /// The maximum size of a tuple that can be stored in a page
    pub const MAX_SIZE: u16 =
        PAGE_DATA_SIZE as u16 - HeapTuplePageHeader::SERIALIZED_SIZE - Slot::SERIALIZED_SIZE;
}

impl HeapTuple {}

#[derive(Debug, PartialEq, rkyv::Archive, rkyv::Serialize)]
struct HeapTupleHeader {}

#[cfg(test)]
mod tests;
