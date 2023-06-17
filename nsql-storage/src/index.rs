use nsql_storage_engine::{ExecutionMode, ReadWriteExecutionMode, StorageEngine};

use crate::eval::TupleExpr;
use crate::table_storage::PrimaryKeyConflict;
use crate::tuple::Tuple;
use crate::{TableStorage, TableStorageInfo};

pub(crate) struct IndexStorage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: TableStorage<'env, 'txn, S, M>,
    index_expr: TupleExpr,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> IndexStorage<'env, 'txn, S, M> {
    pub fn open(
        storage: &S,
        tx: M::TransactionRef<'txn>,
        info: IndexStorageInfo,
    ) -> Result<Self, S::Error> {
        let storage = TableStorage::open(storage, tx, info.table, vec![])?;
        Ok(Self { storage, index_expr: info.index_expr })
    }
}

impl<'env, 'txn, S: StorageEngine> IndexStorage<'env, 'txn, S, ReadWriteExecutionMode> {
    pub fn insert(&mut self, tuple: &Tuple) -> Result<(), anyhow::Error> {
        let tuple = self.index_expr.eval(tuple)?;
        self.storage
            .insert(&tuple)?
            .map_err(|PrimaryKeyConflict { key }| anyhow::anyhow!("unique index conflict: {}", key))
    }
}

pub struct IndexStorageInfo {
    table: TableStorageInfo,
    index_expr: TupleExpr,
}

impl IndexStorageInfo {
    pub fn new(table: TableStorageInfo, index_expr: TupleExpr) -> Self {
        Self { table, index_expr }
    }
}
