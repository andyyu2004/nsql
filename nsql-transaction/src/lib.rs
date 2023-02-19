use std::sync::atomic::{self, AtomicU64};

pub struct TransactionManager {
    current_txid: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self { current_txid: AtomicU64::new(0) }
    }

    pub fn begin(&self) -> Transaction {
        Transaction::new(self.next_txid())
    }
}

impl TransactionManager {
    fn next_txid(&self) -> Txid {
        let id = self.current_txid.fetch_add(1, atomic::Ordering::SeqCst);
        Txid(id)
    }
}

/// Opaque transaction ID
pub struct Txid(u64);

pub struct Transaction {
    txid: Txid,
}

impl Transaction {
    fn new(txid: Txid) -> Transaction {
        Self { txid }
    }
}
