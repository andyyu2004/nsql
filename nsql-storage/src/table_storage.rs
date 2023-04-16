mod fsm;
mod heap;

use std::sync::Arc;

use nsql_buffer::Pool;
use nsql_core::schema::Schema;
use nsql_pager::PageIndex;
use nsql_transaction::Transaction;

use self::heap::Heap;
use crate::tuple::Tuple;

pub struct TableStorage {
    heap: Heap<Tuple>,
    info: TableStorageInfo,
}

impl TableStorage {
    pub async fn initialize(
        pool: Arc<dyn Pool>,
        info: TableStorageInfo,
    ) -> nsql_buffer::Result<Self> {
        let heap = Heap::initialize(Arc::clone(&pool)).await?;
        Ok(Self { heap, info })
    }

    pub async fn append(&self, tx: &Transaction, tuple: &Tuple) -> nsql_buffer::Result<()> {
        self.heap.append(tx, tuple).await?;
        Ok(())
    }

    pub async fn scan(&self, tx: &Transaction) -> nsql_buffer::Result<Vec<Tuple>> {
        let tuples = self.heap.scan(tx).await?;
        Ok(tuples)
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

#[derive(Debug, PartialEq, rkyv::Archive, rkyv::Serialize)]
struct HeapTuple {
    tuple: Tuple,
}

#[cfg(test)]
mod tests;
