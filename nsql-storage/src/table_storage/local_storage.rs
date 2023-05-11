use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Weak};

use nsql_transaction::{Transaction, Transactional, Txid};
use rkyv::AlignedVec;
use tokio::sync::RwLock;

use super::heap::Heap;
use crate::table_storage::TupleId;
use crate::tuple::Tuple;

/// Transaction local storage that stores uncommitted changes
pub(super) struct LocalStorage {
    tx: Weak<Transaction>,
    finished: AtomicBool,
    /// The heap to apply changes to on commit
    heap: Arc<Heap<Tuple>>,
    /// The changes to apply to the heap on commit
    changes: RwLock<Vec<Change>>,
}

impl LocalStorage {
    // We only store a weak reference to the transaction to avoid a reference cycle
    pub fn new(tx: Weak<Transaction>, heap: Arc<Heap<Tuple>>) -> Self {
        Self { tx, heap, finished: AtomicBool::new(false), changes: Default::default() }
    }

    pub async fn update(&self, tid: TupleId, tuple: &Tuple) -> nsql_buffer::Result<()> {
        assert!(!self.finished.load(atomic::Ordering::Acquire));

        let raw_tuple = self.heap.serialize_tuple(&self.tx(), tuple);
        self.changes.write().await.push(Change::Update { tid, raw_tuple });
        Ok(())
    }

    fn tx(&self) -> Arc<Transaction> {
        self.tx.upgrade().expect("transaction dropped before storage")
    }
}

#[async_trait::async_trait]
impl Transactional for LocalStorage {
    async fn commit(&self, tx: &Transaction) -> anyhow::Result<()> {
        assert_eq!(tx.id(), self.tx().id());
        self.finished.store(true, atomic::Ordering::Release);

        let mut changes = self.changes.write().await;
        for change in std::mem::take(&mut *changes) {
            match change {
                // FIXME need to handle errors by rolling back the transaction?
                Change::Update { tid, raw_tuple } => {
                    unsafe { self.heap.update_raw(tx, tid, &raw_tuple) }
                        .await
                        .map_err(|report| report.into_error())?;
                }
            }
        }

        Ok(())
    }

    async fn rollback(&self, tx: &Transaction) {
        assert_eq!(tx.id(), self.tx().id());
        // drop all changes on rollback and free used memory
        let mut changes = self.changes.write().await;
        changes.clear();
        changes.shrink_to_fit();
    }
}

#[derive(Debug)]
enum Change {
    Update { tid: TupleId, raw_tuple: AlignedVec },
}
