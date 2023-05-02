mod fsm;
mod heap;

use std::sync::Arc;

use futures_util::Stream;
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

    pub async fn scan(
        &self,
        tx: Arc<Transaction>,
    ) -> impl Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send {
        self.heap.scan(tx).await
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
