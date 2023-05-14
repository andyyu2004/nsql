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
use rkyv::option::ArchivedOption;
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
    next_xid: AtomicU64,
    active: SkipSet<Xid>,
    // FIXME need to gc these transactions
    transactions: SkipMap<Xid, Arc<Transaction>>,
}

impl Default for Shared {
    fn default() -> Self {
        const START_xid: u64 = 1;
        Self {
            min_active: AtomicU64::new(START_xid),
            next_xid: AtomicU64::new(START_xid),
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
        let xid = self.next_xid();
        debug_assert!(self.shared.active.get(&xid).is_none());
        debug_assert!(self.shared.transactions.get(&xid).is_none());
        self.shared.active.insert(xid);
        let tx = Arc::new(Transaction::new(self, xid));
        self.shared.transactions.insert(xid, Arc::clone(&tx));
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
    fn next_xid(&self) -> Xid {
        Xid::new(self.shared.next_xid.fetch_add(1, atomic::Ordering::AcqRel))
    }

    #[inline]
    fn snapshot(&self) -> TransactionSnapshot {
        // loading in this particular order to ensure that the assertions in `TransactionSnapshot::new` hold
        // (i.e. that xmin <= xid < xmax)
        let min_active = Xid::new(self.shared.min_active.load(atomic::Ordering::Acquire));
        let active = self.shared.active.iter().map(|entry| *entry.value()).collect();
        let next_xid = Xid::new(self.shared.next_xid.load(atomic::Ordering::Acquire));
        TransactionSnapshot::new(min_active, active, next_xid)
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
pub struct Xid(NonZeroU64);

impl From<Archived<Xid>> for Xid {
    #[inline]
    fn from(xid: Archived<Xid>) -> Self {
        Self(xid.0.into())
    }
}

impl From<Xid> for Archived<Xid> {
    #[inline]
    fn from(value: Xid) -> Self {
        Self(value.0.into())
    }
}

// Should be 8 bytes due to niche optimization
static_assert_eq!(std::mem::size_of::<Option<Xid>>(), 8);

impl Xid {
    #[inline]
    pub fn new(xid: u64) -> Self {
        Self(NonZeroU64::new(xid).unwrap())
    }
}

impl fmt::Debug for Xid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Xid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct TransactionSnapshot {
    /// The lowest transaction id that is still active. All transactions with a lower id have completed.
    min_active: Xid,
    /// The next transaction id to be assigned. This is monotonically increasing.
    next_xid: Xid,
    /// The set of active transactions
    active: HashSet<Xid>,
}

impl fmt::Debug for TransactionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.min_active, self.next_xid, self.active.iter().join(","),)
    }
}

impl TransactionSnapshot {
    fn new(min_active: Xid, active: HashSet<Xid>, next_xid: Xid) -> Self {
        active.iter().for_each(|&xid| {
            assert!(min_active <= xid);
            assert!(xid < next_xid, "xid={} !< next_xid={}", xid.0, next_xid.0);
        });
        Self { min_active, next_xid, active }
    }

    #[inline]
    pub fn min_active(&self) -> Xid {
        self.min_active
    }

    #[inline]
    pub fn next_xid(&self) -> Xid {
        self.next_xid
    }

    #[inline]
    pub fn active(&self) -> &HashSet<Xid> {
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
    xmin: Xid,
    /// The transaction id that modified/deleted this version
    // FIXME niche optimize this for rkyv
    xmax: Option<Xid>,
}

impl From<Archived<Version>> for Version {
    fn from(value: Archived<Version>) -> Self {
        Self { xmin: value.xmin.into(), xmax: value.xmax.as_ref().map(|v| (*v).into()) }
    }
}

impl From<Version> for Archived<Version> {
    fn from(value: Version) -> Self {
        Self {
            xmin: value.xmin.into(),
            xmax: match value.xmax {
                Some(xmax) => ArchivedOption::Some(xmax.into()),
                None => ArchivedOption::None,
            },
        }
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

static_assert!(Atomic::<Option<Xid>>::is_lock_free());

impl Version {
    #[inline]
    pub fn xmin(&self) -> Xid {
        self.xmin
    }

    #[inline]
    pub fn xmax(&self) -> Option<Xid> {
        self.xmax
    }

    /// Returns a new version that represents a delete operation by the given transaction
    #[inline]
    pub fn delete_with(&self, tx: &Transaction) -> Version {
        assert!(self.xmin <= tx.xid());
        assert!(self.xmax.is_none(), "cannot delete a version twice");
        Version { xmin: self.xmin, xmax: Some(tx.xid()) }
    }
}

pub struct Transaction {
    id: Xid,
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
    fn new(txm: &TransactionManager, id: Xid) -> Transaction {
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
    pub fn xid(&self) -> Xid {
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
                            xmax >= self.snapshot.next_xid()
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
            dep.commit(self).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn rollback(&self) {
        self.complete(TransactionState::RolledBack);
        for dep in self.dependencies.read().await.iter() {
            dep.rollback(self).await;
        }
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
                .unwrap_or_else(|| Xid::new(self.shared.next_xid.load(atomic::Ordering::Acquire)));

            let prev = self.shared.min_active.swap(next_xmin.0.get(), atomic::Ordering::Release);
            assert!(prev <= next_xmin.0.get(), "xmin should be monotonically increasing");

            self.state.store(final_state, atomic::Ordering::Release)
        })
    }
}

#[async_trait::async_trait]
pub trait Transactional: Send + Sync + 'static {
    async fn commit(&self, tx: &Transaction) -> anyhow::Result<()>;

    /// This intentionally does not return a result as we do not want `rollback` to do anything fallible.
    async fn rollback(&self, tx: &Transaction);

    async fn cleanup(&self, tx: &Transaction) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl<T: Transactional> Transactional for Arc<T> {
    #[inline]
    async fn commit(&self, tx: &Transaction) -> anyhow::Result<()> {
        (**self).commit(tx).await
    }

    #[inline]
    async fn rollback(&self, tx: &Transaction) {
        (**self).rollback(tx).await
    }

    #[inline]
    async fn cleanup(&self, tx: &Transaction) -> anyhow::Result<()> {
        (**self).cleanup(tx).await
    }
}

#[cfg(test)]
mod tests;
