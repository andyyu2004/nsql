use std::collections::HashMap;
use std::mem;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Weak};

use nsql_transaction::{Transaction, Transactional, Txid};
use parking_lot::RwLock;
use rkyv::AlignedVec;

use super::heap::Heap;
use crate::table_storage::TupleId;
use crate::tuple::Tuple;

/// Transaction local storage that stores uncommitted changes
pub(super) struct LocalStorage {
    tx: Weak<Transaction>,
    finished: AtomicBool,
    /// The heap to apply changes to on commit
    heap: Arc<Heap<Tuple>>,
    updates: RwLock<HashMap<TupleId, Vec<Update>>>,
    inserts: RwLock<Vec<Insert>>,
}

impl LocalStorage {
    // We only store a weak reference to the transaction to avoid a reference cycle
    pub fn new(tx: Weak<Transaction>, heap: Arc<Heap<Tuple>>) -> Self {
        Self {
            tx,
            heap,
            finished: AtomicBool::new(false),
            updates: Default::default(),
            inserts: Default::default(),
        }
    }

    pub fn append(&self, tuple: &Tuple) {
        assert!(!self.finished.load(atomic::Ordering::Acquire));

        let raw_tuple = self.heap.serialize_tuple(&self.tx(), tuple);
        self.inserts.write().push(Insert { raw_tuple });
    }

    pub fn update(&self, tid: TupleId, tuple: &Tuple) {
        assert!(!self.finished.load(atomic::Ordering::Acquire));

        let raw_tuple = self.heap.serialize_tuple(&self.tx(), tuple);
        self.updates.write().entry(tid).or_default().push(Update { raw_tuple });
    }

    /// Apply local updates to the committed tuple
    pub fn patch(&self, xid: Txid, tid: TupleId, tuple: &mut Tuple) {
        assert_eq!(self.tx().xid(), xid);
        // since updates are not patches but just full copies currently, we just read the latest
        if let Some(latest_update) =
            self.updates.read().get(&tid).and_then(|updates| updates.last())
        {
            let rkyv_tuple = unsafe { rkyv::archived_root::<Tuple>(&latest_update.raw_tuple) };
            *tuple = nsql_rkyv::deserialize(rkyv_tuple);
        }
    }

    fn tx(&self) -> Arc<Transaction> {
        self.tx.upgrade().expect("transaction dropped before storage")
    }
}

#[async_trait::async_trait]
impl Transactional for LocalStorage {
    async fn commit(&self, tx: &Transaction) -> anyhow::Result<()> {
        assert_eq!(tx.xid(), self.tx().xid());
        self.finished.store(true, atomic::Ordering::Release);

        // FIXME need to handle errors by rolling back the transaction?
        let updates = mem::take(&mut *self.updates.write());
        for (tid, updates) in updates {
            for update in updates {
                unsafe { self.heap.update_raw(tx, tid, &update.raw_tuple) }
                    .await
                    .map_err(|report| report.into_error())?;
            }
        }

        let inserts = mem::take(&mut *self.inserts.write());
        for insert in inserts {
            unsafe { self.heap.append_raw(tx, &insert.raw_tuple) }
                .await
                .map_err(|report| report.into_error())?;
        }

        Ok(())
    }

    async fn rollback(&self, tx: &Transaction) {
        assert_eq!(tx.xid(), self.tx().xid());
        // drop all changes on rollback and free used memory

        let mut changes = self.updates.write();
        changes.clear();
        changes.shrink_to_fit();

        let mut inserts = self.inserts.write();
        inserts.clear();
        inserts.shrink_to_fit();
    }

    async fn cleanup(&self, _tx: &Transaction) -> anyhow::Result<()> {
        todo!()
    }
}

#[derive(Debug)]
struct Insert {
    raw_tuple: AlignedVec,
}

#[derive(Debug)]
struct Update {
    raw_tuple: AlignedVec,
}
