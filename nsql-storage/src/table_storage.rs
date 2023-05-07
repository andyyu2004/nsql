mod fsm;
mod heap;

use std::sync::Arc;

use futures_util::Stream;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_transaction::Transaction;

use self::heap::{Heap, HeapId};
use crate::schema::Schema;
use crate::tuple::{ColumnIndex, Tuple};
use crate::value::Value;

pub struct TableStorage {
    heap: Heap<Tuple>,
    info: TableStorageInfo,
}

pub type TupleId = HeapId<Tuple>;

impl TableStorage {
    #[inline]
    pub async fn initialize(
        pool: Arc<dyn Pool>,
        info: TableStorageInfo,
    ) -> nsql_buffer::Result<Self> {
        let heap = Heap::initialize(Arc::clone(&pool)).await?;
        Ok(Self { heap, info })
    }

    #[inline]
    pub async fn append(&self, tx: &Transaction, tuple: &Tuple) -> nsql_buffer::Result<TupleId> {
        self.heap.append(tx, tuple).await
    }

    #[inline]
    pub async fn update(
        &self,
        tx: &Transaction,
        id: TupleId,
        tuple: &Tuple,
    ) -> nsql_buffer::Result<()> {
        todo!();
        Ok(())
    }

    #[inline]
    pub async fn scan(
        &self,
        tx: Arc<Transaction>,
        projections: Option<&[ColumnIndex]>,
    ) -> impl Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send {
        // Add the tuple id as an additional column to the end of the tuple
        self.heap.scan(tx, projections).await
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

#[cfg(test)]
mod tests;
