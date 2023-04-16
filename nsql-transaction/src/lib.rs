use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

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

#[derive(Debug)]
pub struct Transaction {
    id: Txid,
}

impl Transaction {
    fn new(id: Txid) -> Transaction {
        Self { id }
    }

    pub fn id(&self) -> Txid {
        self.id
    }

    pub fn can_see(&self, other: Txid) -> bool {
        self.id.0 >= other.0
    }

    pub async fn commit(self) {}

    pub async fn commit_arc(self: Arc<Self>) {
        Arc::try_unwrap(self)
            .expect("outstanding references to committing transaction")
            .commit()
            .await
    }

    pub async fn rollback(self) {}
}
