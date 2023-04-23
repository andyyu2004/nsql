use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::num::NonZeroU64;
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

#[derive(Debug)]
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

impl Default for Shared {
    fn default() -> Self {
        const START_TXID: u64 = 1;
        Self {
            xmin: AtomicU64::new(START_TXID),
            xmax: AtomicU64::new(START_TXID),
            active: SkipSet::new(),
            transactions: SkipMap::new(),
        }
    }
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
        Txid::new(self.shared.xmax.fetch_add(1, atomic::Ordering::AcqRel))
    }

    #[inline]
    fn snapshot(&self) -> TransactionSnapshot {
        // loading in this particular order to ensure that the assertions in `TransactionSnapshot::new` hold
        // (i.e. that xmin <= txid < xmax)
        let xmin = Txid::new(self.shared.xmin.load(atomic::Ordering::Acquire));
        let active = self.shared.active.iter().map(|entry| *entry.value());
        let xmax = Txid::new(self.shared.xmax.load(atomic::Ordering::Acquire));
        TransactionSnapshot::new(xmin, active, xmax)
    }
}

/// Opaque monotonically increasing transaction id
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Txid(NonZeroU64);

impl Txid {
    #[inline]
    pub fn new(txid: u64) -> Self {
        Self(NonZeroU64::new(txid).unwrap())
    }
}

#[derive(Debug)]
pub struct TransactionSnapshot {
    /// The lowest transaction id that is still active. All transactions with a lower id have completed.
    xmin: Txid,
    /// The next transaction id to be assigned. This is monotonically increasing.
    xmax: Txid,
    /// The set of active transactions
    active: HashSet<Txid>,
}

impl TransactionSnapshot {
    fn new(xmin: Txid, active: impl IntoIterator<Item = Txid>, xmax: Txid) -> Self {
        let active = active
            .into_iter()
            .inspect(|&txid| {
                assert!(xmin <= txid);
                assert!(txid < xmax, "txid={} !< xmax={}", txid.0, xmax.0);
            })
            .collect();
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
    pub fn active(&self) -> &HashSet<Txid> {
        &self.active
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Started,
    Committed,
    Aborted,
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

#[derive(Debug, Clone, Copy)]
pub struct Version {
    /// The transaction id that created this version
    xmin: Txid,
    /// The transaction id that modified/deleted this version
    xmax: Option<Txid>,
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
    pub fn version(&self) -> Version {
        Version { xmin: self.id, xmax: None }
    }

    #[inline]
    pub fn can_see(&self, version: Version) -> bool {
        // none of the xmax is not None cases are checked
        assert!(version.xmax.is_none(), "not implemented");

        if self.id == version.xmin {
            // We created this version, so we can see it
            // FIXME handle xmax logic
            return true;
        }

        // The transaction that created this version is still active in our snapshot, `version` is not visible
        if self.snapshot.active.contains(&version.xmin) {
            return false;
        }

        let tx = self.shared.transactions.get(&version.xmin).unwrap();
        match tx.value().state() {
            TransactionState::Started => {
                unreachable!("transaction is active but not in snapshot's active set")
            }
            // The transaction that created this version committed, `version` is visible
            TransactionState::Committed => true,
            // The transaction that created this version was aborted, `version` is not visible
            TransactionState::Aborted => false,
            TransactionState::RolledBack => todo!(),
        }
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
                // if there are no active transactions, then we set the next xmin to the current xmax
                .unwrap_or_else(|| Txid::new(self.shared.xmax.load(atomic::Ordering::Acquire)));

            let prev = self.shared.xmin.swap(next_xmin.0.get(), atomic::Ordering::Release);
            assert!(prev <= next_xmin.0.get(), "xmin should be monotonically increasing");

            self.state.store(final_state, atomic::Ordering::Release)
        })
    }
}
