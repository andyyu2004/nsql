use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{self, AtomicBool, AtomicU64};
use std::sync::{Arc, Once};

use crossbeam_skiplist::{SkipMap, SkipSet};
use nsql_util::atomic::AtomicEnum;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("cannot start transaction from within a transaction")]
    TransactionAlreadyStarted,
    #[error("cannot commit transaction that has not been started")]
    CommitWithoutTransaction,
    #[error("cannot rollback transaction that has not been started")]
    RollbackWithoutTransaction,
}

#[derive(Clone)]
pub struct TransactionManager {
    shared: Arc<Shared>,
}

#[derive(Debug, Default)]
struct Shared {
    /// The lowest transaction id that is still active. All transactions with a lower id have completed.
    /// This is monotonically increasing.
    xmin: AtomicU64,
    /// The next transaction id to be assigned. This is monotonically increasing.
    /// This can also be interpreted as the (exclusive) upper bound of the transaction id.
    xmax: AtomicU64,
    active: SkipSet<Txid>,
    // FIXME need to gc these transactions
    transactions: SkipMap<Txid, Arc<Transaction>>,
}

impl TransactionManager {
    #[inline]
    pub fn initialize() -> Self {
        Self { shared: Arc::default() }
    }

    #[inline]
    pub fn begin(&self) -> Arc<Transaction> {
        let txid = self.next_txid();
        self.shared.active.insert(txid);
        let tx = Arc::new(Transaction::new(self, txid));
        self.shared.transactions.insert(txid, Arc::clone(&tx));
        tx
    }
}

impl Default for TransactionManager {
    #[inline]
    fn default() -> Self {
        Self::initialize()
    }
}

impl TransactionManager {
    fn next_txid(&self) -> Txid {
        Txid(self.shared.xmax.fetch_add(1, atomic::Ordering::AcqRel))
    }

    #[inline]
    fn snapshot(&self) -> TransactionSnapshot {
        TransactionSnapshot::new(
            Txid(self.shared.xmin.load(atomic::Ordering::Acquire)),
            Txid(self.shared.xmax.load(atomic::Ordering::Acquire)),
            self.shared.active.iter().map(|entry| *entry.value()),
        )
    }
}

/// Opaque monotonically increasing transaction id
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Txid(u64);

#[derive(Debug)]
pub struct TransactionSnapshot {
    xmin: Txid,
    xmax: Txid,
    active: BTreeSet<Txid>,
}

impl TransactionSnapshot {
    fn new(xmin: Txid, xmax: Txid, active: impl IntoIterator<Item = Txid>) -> Self {
        let active =
            active.into_iter().inspect(|&txid| assert!(xmin <= txid && txid < xmax)).collect();
        Self { xmin, xmax, active }
    }

    #[inline]
    pub fn xmin(&self) -> Txid {
        self.xmin
    }

    #[inline]
    pub fn xmax(&self) -> Txid {
        self.xmax
    }

    #[inline]
    pub fn active(&self) -> &BTreeSet<Txid> {
        &self.active
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Started,
    Aborted,
    Committed,
    RolledBack,
}

impl From<u8> for TransactionState {
    fn from(value: u8) -> Self {
        assert!(value <= Self::RolledBack as u8);
        unsafe { std::mem::transmute(value) }
    }
}

impl From<TransactionState> for u8 {
    fn from(value: TransactionState) -> Self {
        value as u8
    }
}

pub struct Transaction {
    id: Txid,
    once: Once,
    shared: Arc<Shared>,
    state: AtomicEnum<TransactionState>,
    auto_commit: AtomicBool,
    snapshot: TransactionSnapshot,
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("state", &self.state())
            .field("auto_commit", &self.auto_commit())
            .finish()
    }
}

impl Transaction {
    #[inline]
    fn new(txm: &TransactionManager, id: Txid) -> Transaction {
        assert!(
            txm.shared.active.contains(&id),
            "must add transaction to active set before creating it"
        );

        Self {
            id,
            once: Once::new(),
            shared: Arc::clone(&txm.shared),
            state: AtomicEnum::new(TransactionState::Started),
            auto_commit: AtomicBool::new(true),
            snapshot: txm.snapshot(),
        }
    }

    #[inline]
    pub fn id(&self) -> Txid {
        self.id
    }

    #[inline]
    pub fn can_see(&self, txid: Txid) -> bool {
        if self.id == txid {
            // We can always see our own operations
            return true;
        }

        self.snapshot.xmin <= txid
            && txid < self.snapshot.xmax
            && !self.snapshot.active.contains(&txid)
            && matches!(
                self.shared.transactions.get(&txid).unwrap().value().state(),
                TransactionState::Committed
            )
    }

    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn set_auto_commit(&self, auto_commit: bool) {
        self.auto_commit.store(auto_commit, atomic::Ordering::Release)
    }

    #[inline]
    pub fn state(&self) -> TransactionState {
        self.state.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn commit(&self) {
        self.complete(TransactionState::Committed);
    }

    #[inline]
    pub fn rollback(&self) {
        self.complete(TransactionState::RolledBack);
    }

    fn complete(&self, final_state: TransactionState) {
        self.once.call_once(|| {
            assert!(
                self.shared.active.remove(&self.id).is_some(),
                "attempted to rollback a transaction that was not active"
            );

            let next_xmin = self
                .shared
                .active
                .front()
                .map(|entry| *entry.value())
                // if there are no active transactions, the we set the next xmin to the current xmax
                .unwrap_or_else(|| Txid(self.shared.xmin.load(atomic::Ordering::Acquire)));

            // if current `xmin` is ourselves, we need to update it
            let _ = self.shared.xmin.compare_exchange(
                self.id.0,
                next_xmin.0,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            );

            self.state.store(final_state, atomic::Ordering::Release)
        })
    }
}
