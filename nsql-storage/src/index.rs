use nsql_storage_engine::{ExecutionMode, ReadWriteExecutionMode, StorageEngine};

use crate::{TableStorage, TableStorageInfo};

pub(crate) struct IndexStorage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: TableStorage<'env, 'txn, S, M>,
}

impl<'env, 'txn, S: StorageEngine> IndexStorage<'env, 'txn, S, ReadWriteExecutionMode> {
    #[inline]
    pub fn create(
        storage: &S,
        tx: &'txn S::WriteTransaction<'env>,
        info: IndexStorageInfo,
    ) -> Result<Self, S::Error> {
        // create the tree
        storage.open_write_tree(tx, info.table.name())?;
        let storage = TableStorage::open(storage, tx, info.table, vec![])?;
        Ok(Self { storage })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> IndexStorage<'env, 'txn, S, M> {
    pub fn open(
        storage: &S,
        tx: M::TransactionRef<'txn>,
        info: IndexStorageInfo,
    ) -> Result<Self, S::Error> {
        let storage = TableStorage::open(storage, tx, info.table, vec![])?;
        Ok(Self { storage })
    }
}

pub struct IndexStorageInfo {
    table: TableStorageInfo,
}
