#![deny(rust_2018_idioms)]

use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Once};

use atomic::Atomic;
use crossbeam_skiplist::{SkipMap, SkipSet};
use itertools::Itertools;
use nsql_util::atomic::AtomicEnum;
use nsql_util::{static_assert, static_assert_eq};
use rkyv::{Archive, Archived, Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum TransactionError {
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
    min_active: AtomicU64,
    /// The next transaction id to be assigned. This is monotonically increasing.
    /// This can also be interpreted as the (exclusive) upper bound of the transaction id.
    next_txid: AtomicU64,
    active: SkipSet<Txid>,
    // FIXME need to gc these transactions
    transactions: SkipMap<Txid, Arc<Transaction>>,
}

impl Default for Shared {
    fn default() -> Self {
        const START_TXID: u64 = 1;
        Self {
            min_active: AtomicU64::new(START_TXID),
            next_txid: AtomicU64::new(START_TXID),
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
        debug_assert!(self.shared.active.get(&txid).is_none());
        debug_assert!(self.shared.transactions.get(&txid).is_none());
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
        Txid::new(self.shared.next_txid.fetch_add(1, atomic::Ordering::AcqRel))
    }

    #[inline]
    fn snapshot(&self) -> TransactionSnapshot {
        // loading in this particular order to ensure that the assertions in `TransactionSnapshot::new` hold
        // (i.e. that xmin <= txid < xmax)
        let min_active = Txid::new(self.shared.min_active.load(atomic::Ordering::Acquire));
        let active = self.shared.active.iter().map(|entry| *entry.value()).collect();
        let next_txid = Txid::new(self.shared.next_txid.load(atomic::Ordering::Acquire));
        TransactionSnapshot::new(min_active, active, next_txid)
    }
}

/// Opaque monotonically increasing transaction id
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord))]
#[archive(compare(PartialOrd, PartialEq))]
#[repr(transparent)]
pub struct Txid(NonZeroU64);

impl From<Archived<Txid>> for Txid {
    #[inline]
    fn from(txid: Archived<Txid>) -> Self {
        Self(txid.0.into())
    }
}

// Should be 8 bytes due to niche optimization
static_assert_eq!(std::mem::size_of::<Option<Txid>>(), 8);

impl Txid {
    #[inline]
    pub fn new(txid: u64) -> Self {
        Self(NonZeroU64::new(txid).unwrap())
    }
}

impl fmt::Debug for Txid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Txid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct TransactionSnapshot {
    /// The lowest transaction id that is still active. All transactions with a lower id have completed.
    min_active: Txid,
    /// The next transaction id to be assigned. This is monotonically increasing.
    next_txid: Txid,
    /// The set of active transactions
    active: HashSet<Txid>,
}

impl fmt::Debug for TransactionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.min_active, self.next_txid, self.active.iter().join(","),)
    }
}

impl TransactionSnapshot {
    fn new(min_active: Txid, active: HashSet<Txid>, next_txid: Txid) -> Self {
        active.iter().for_each(|&txid| {
            assert!(min_active <= txid);
            assert!(txid < next_txid, "txid={} !< next_txid={}", txid.0, next_txid.0);
        });
        Self { min_active, next_txid, active }
    }

    #[inline]
    pub fn min_active(&self) -> Txid {
        self.min_active
    }

    #[inline]
    pub fn next_txid(&self) -> Txid {
        self.next_txid
    }

    #[inline]
    pub fn active(&self) -> &HashSet<Txid> {
        &self.active
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Active,
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

#[derive(Copy, Clone, Archive, Serialize, Deserialize)]
#[archive_attr(derive(Copy, Clone, Debug))]
pub struct Version {
    /// The transaction id that created this version
    xmin: Txid,
    /// The transaction id that modified/deleted this version
    // FIXME niche optimize this for rkyv
    xmax: Option<Txid>,
}

impl From<Archived<Version>> for Version {
    fn from(value: Archived<Version>) -> Self {
        Self { xmin: value.xmin.into(), xmax: value.xmax.as_ref().map(|v| (*v).into()) }
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.xmax() {
            None => write!(f, "{}:∞", self.xmin),
            Some(xmax) => write!(f, "{}:{}", self.xmin, xmax),
        }
    }
}

static_assert!(Atomic::<Option<Txid>>::is_lock_free());

impl Version {
    #[inline]
    pub fn xmin(&self) -> Txid {
        self.xmin
    }

    #[inline]
    pub fn xmax(&self) -> Option<Txid> {
        self.xmax
    }

    /// Returns a new version that represents a delete operation by the given transaction
    #[inline]
    pub fn delete_with(&self, tx: &Transaction) -> Version {
        assert!(self.xmin <= tx.id());
        assert!(self.xmax.is_none(), "cannot delete a version twice");
        Version { xmin: self.xmin, xmax: Some(tx.id()) }
    }
}

pub struct Transaction {
    id: Txid,
    once: Once,
    shared: Arc<Shared>,
    state: AtomicEnum<TransactionState>,
    auto_commit: AtomicBool,
    snapshot: TransactionSnapshot,
    dependencies: RwLock<Vec<Box<dyn Transactional>>>,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        assert!(self.once.is_completed(), "transaction must be completed before dropping");
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("id", &self.id)
            .field("state", &self.state())
            .field("snapshot", &self.snapshot)
            .finish_non_exhaustive()
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
            state: AtomicEnum::new(TransactionState::Active),
            auto_commit: AtomicBool::new(true),
            snapshot: txm.snapshot(),
            dependencies: Default::default(),
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

    /// Returns whether `version` is visible to this transaction
    #[tracing::instrument]
    pub fn can_see(&self, version: Version) -> bool {
        debug_assert_eq!(
            self.shared.transactions.get(&self.id).unwrap().value().state(),
            TransactionState::Active,
        );

        // FIXME these needs a large optimization pass as it will be an extremely hot path
        // We should avoid reading from `shared` until strictly necessary

        let xmin_tx = self.shared.transactions.get(&version.xmin).unwrap();
        let is_visible = match xmin_tx.value().state() {
            // The transaction that created this version is still active, `version` is not visible
            // unless we are the one who created it and we have not since deleted it
            TransactionState::Active => {
                if let Some(xmax) = version.xmax() {
                    assert_eq!(
                        self.id, xmax,
                        "another transaction deleted a version we created before we have committed it"
                    );
                }

                self.id == version.xmin && version.xmax().is_none()
            }
            // The transaction that created this version committed, `version` may be visible
            TransactionState::Committed => match version.xmax() {
                // The version has not been deleted since it was created, `version` is visible if both of the following hold:
                // - The transaction that created it committed before we started
                // - The transaction was not active while we took our snapshot
                // We need both conditions as they do not rule out the case where the transaction
                // started after us (and therefore is not in our snapshot), but committed before us
                None => version.xmin < self.id && !self.snapshot.active().contains(&version.xmin),
                Some(xmax) => {
                    let xmax_tx = self.shared.transactions.get(&xmax).unwrap();
                    match xmax_tx.value().state() {
                        // The transaction that deleted this version is still active, `version` is still visible,
                        // unless we were the one who deleted it
                        TransactionState::Active => xmax != self.id,
                        // The transaction that deleted this version committed, `version` is visible if either
                        // - The transaction started after the snapshot was taken
                        // - The transaction was active while we took our snapshot
                        TransactionState::Committed => {
                            xmax >= self.snapshot.next_txid()
                                || self.snapshot.active().contains(&xmax)
                        }
                        // The transaction that deleted this version aborted, `version` is still visible
                        TransactionState::Aborted | TransactionState::RolledBack => true,
                    }
                }
            },
            // The transaction that created this version aborted, `version` is not visible
            TransactionState::Aborted | TransactionState::RolledBack => false,
        };

        tracing::debug!(is_visible);
        is_visible
    }

    #[inline]
    pub async fn add_dependency<T: Transactional>(&self, dep: T) {
        self.dependencies.write().await.push(Box::new(dep));
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
    pub async fn commit(&self) -> anyhow::Result<()> {
        self.complete(TransactionState::Committed);
        for dep in self.dependencies.read().await.iter() {
            dep.commit().await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn rollback(&self) -> anyhow::Result<()> {
        self.complete(TransactionState::RolledBack);
        for dep in self.dependencies.read().await.iter() {
            dep.rollback().await?;
        }
        Ok(())
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
                .unwrap_or_else(|| {
                    Txid::new(self.shared.next_txid.load(atomic::Ordering::Acquire))
                });

            let prev = self.shared.min_active.swap(next_xmin.0.get(), atomic::Ordering::Release);
            assert!(prev <= next_xmin.0.get(), "xmin should be monotonically increasing");

            self.state.store(final_state, atomic::Ordering::Release)
        })
    }
}

#[async_trait::async_trait]
pub trait Transactional: Send + Sync + 'static {
    async fn commit(&self) -> anyhow::Result<()>;

    async fn rollback(&self) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl<T: Transactional> Transactional for Arc<T> {
    #[inline]
    async fn commit(&self) -> anyhow::Result<()> {
        (**self).commit().await
    }

    #[inline]
    async fn rollback(&self) -> anyhow::Result<()> {
        (**self).rollback().await
    }
}
