mod fsm;
mod heap;
mod local_storage;

use std::sync::{Arc, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use futures_util::{stream, Stream, StreamExt};
use nsql_buffer::Pool;
use nsql_pager::PageIndex;

use self::heap::{Heap, HeapId};
use self::local_storage::LocalStorage;
use crate::schema::Schema;
use crate::tuple::{Tuple, TupleIndex};
use crate::{Transaction, Txid};

pub struct TableStorage {
    info: TableStorageInfo,
    /// The persisted state of the table stored in the heap
    heap: Arc<Heap<Tuple>>,
    local_storages: DashMap<Txid, Weak<LocalStorage>>,
}

pub type TupleId = HeapId<Tuple>;

impl TableStorage {
    #[inline]
    pub async fn initialize(
        pool: Arc<dyn Pool>,
        info: TableStorageInfo,
    ) -> nsql_buffer::Result<Self> {
        let heap = Arc::new(Heap::initialize(Arc::clone(&pool)).await?);
        Ok(Self { info, heap, local_storages: Default::default() })
    }

    #[inline]
    pub async fn append(&self, tx: &Arc<Transaction>, tuple: &Tuple) -> nsql_buffer::Result<()> {
        self.local_storage(tx).await.upgrade().unwrap().append(tuple);
        Ok(())
    }

    #[inline]
    pub async fn update(
        &self,
        tx: &Arc<Transaction>,
        id: TupleId,
        tuple: &Tuple,
    ) -> nsql_buffer::Result<()> {
        self.local_storage(tx).await.upgrade().unwrap().update(id, tuple);
        Ok(())
    }

    #[inline]
    pub async fn scan(
        &self,
        tx: Arc<Transaction>,
        projection: Option<Box<[TupleIndex]>>,
    ) -> impl Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send {
        let local_storage = self.local_storage(&tx).await.upgrade().unwrap();
        let local_tuples = local_storage.scan();

        self.heap
            .scan(tx, move |tid, tuple| {
                let mut tuple = match &projection {
                    Some(projection) => tuple.project(tid, projection),
                    None => nsql_rkyv::deserialize(tuple),
                };

                // apply any transaction local updates
                local_storage.patch(tid, &mut tuple);
                tuple
            })
            .await
            .chain(stream::iter(Some(Ok(local_tuples))))
    }

    async fn local_storage(&self, tx: &Arc<Transaction>) -> RefMut<'_, Txid, Weak<LocalStorage>> {
        match self.local_storages.entry(tx.xid()) {
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
        }
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
