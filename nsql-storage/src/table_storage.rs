mod fsm;
mod heap;
mod local_storage;

use std::sync::{Arc, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures_util::Stream;
use nsql_buffer::Pool;
use nsql_pager::PageIndex;
use nsql_transaction::{Transaction, Txid};

use self::heap::{Heap, HeapId};
use self::local_storage::LocalStorage;
use crate::schema::Schema;
use crate::tuple::{Tuple, TupleIndex};

pub struct TableStorage {
    info: TableStorageInfo,
    /// The persisted state of the table stored in the heap
    heap: Arc<Heap<Tuple>>,
    tx_local_storage: DashMap<Txid, Weak<LocalStorage>>,
}

pub type TupleId = HeapId<Tuple>;

impl TableStorage {
    #[inline]
    pub async fn initialize(
        pool: Arc<dyn Pool>,
        info: TableStorageInfo,
    ) -> nsql_buffer::Result<Self> {
        let heap = Arc::new(Heap::initialize(Arc::clone(&pool)).await?);
        Ok(Self { info, heap, tx_local_storage: Default::default() })
    }

    #[inline]
    pub async fn append(&self, tx: &Transaction, tuple: &Tuple) -> nsql_buffer::Result<TupleId> {
        self.heap.append(tx, tuple).await
    }

    #[inline]
    pub async fn update(
        &self,
        tx: &Arc<Transaction>,
        id: TupleId,
        tuple: &Tuple,
    ) -> nsql_buffer::Result<()> {
        let local_storage = match self.tx_local_storage.entry(tx.id()) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => {
                let local_storage =
                    Arc::new(LocalStorage::new(Arc::downgrade(tx), Arc::clone(&self.heap)));
                let r = entry.insert(Arc::downgrade(&local_storage));
                // The transaction owns its local storage, we only only store weak references to it here
                // so it gets dropped with the transaction
                tx.add_dependency(Arc::clone(&local_storage)).await;
                r
            }
        };

        local_storage.upgrade().unwrap().update(id, tuple).await
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
