use std::sync::atomic::{self, AtomicBool, AtomicU64};
use std::sync::Arc;

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

pub struct TransactionManager {
    current_txid: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self { current_txid: AtomicU64::new(0) }
    }

    pub async fn begin(&self) -> Arc<Transaction> {
        Arc::new(Transaction::new(self.next_txid()))
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    fn next_txid(&self) -> Txid {
        let id = self.current_txid.fetch_add(1, atomic::Ordering::SeqCst);
        Txid(id)
    }
}

/// Opaque monotonically increasing transaction id
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Txid(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Started,
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

#[derive(Debug)]
pub struct Transaction {
    id: Txid,
    state: AtomicEnum<TransactionState>,
    auto_commit: AtomicBool,
}

impl Transaction {
    #[inline]
    fn new(id: Txid) -> Transaction {
        Self {
            id,
            state: AtomicEnum::new(TransactionState::Started),
            auto_commit: AtomicBool::new(true),
        }
    }

    #[inline]
    pub fn id(&self) -> Txid {
        self.id
    }

    #[inline]
    pub fn can_see(&self, other: Txid) -> bool {
        self.id.0 >= other.0
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
    pub async fn commit(&self) {
        self.state.store(TransactionState::Committed, atomic::Ordering::Release);
    }

    #[inline]
    pub fn state(&self) -> TransactionState {
        self.state.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub async fn rollback(&self) {
        self.state.store(TransactionState::RolledBack, atomic::Ordering::Release);
    }
}
