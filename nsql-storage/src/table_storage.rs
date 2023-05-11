mod fsm;
mod heap;

use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures_util::Stream;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_transaction::{Transaction, Transactional, Txid};

use self::heap::{Heap, HeapId};
use crate::schema::Schema;
use crate::tuple::{Tuple, TupleIndex};

pub struct TableStorage {
    info: TableStorageInfo,
    /// The persisted state of the table stored in the heap
    heap: Heap<Tuple>,
    tx_local_storage: DashMap<Txid, Arc<LocalStorage>>,
}

#[derive(Debug, Default)]
struct LocalStorage {}

impl LocalStorage {
    async fn update(&self, id: TupleId, tuple: &Tuple) -> nsql_buffer::Result<()> {
        todo!()
    }

    async fn commit(&self) {}
}

#[async_trait::async_trait]
impl Transactional for LocalStorage {
    async fn commit(&self) -> anyhow::Result<()> {
        todo!()
    }

    async fn rollback(&self) -> anyhow::Result<()> {
        todo!()
    }
}

pub type TupleId = HeapId<Tuple>;

impl TableStorage {
    #[inline]
    pub async fn initialize(
        pool: Arc<dyn Pool>,
        info: TableStorageInfo,
    ) -> nsql_buffer::Result<Self> {
        let heap = Heap::initialize(Arc::clone(&pool)).await?;
        Ok(Self { info, heap, tx_local_storage: Default::default() })
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
        let local_storage = match self.tx_local_storage.entry(tx.id()) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => {
                let local_storage = Arc::new(LocalStorage::default());
                tx.add_dependency(Arc::clone(&local_storage)).await;
                entry.insert(Arc::clone(&local_storage))
            }
        };

        local_storage.update(id, tuple).await
        // self.heap.update(tx, id, tuple).await
    }

    #[inline]
    pub async fn scan(
        &self,
        tx: Arc<Transaction>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> impl Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send {
        self.heap
            .scan(tx, move |tid, tuple| match &projection {
                Some(projection) => tuple.project(tid, projection),
                None => nsql_rkyv::deserialize(tuple),
            })
            .await
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
