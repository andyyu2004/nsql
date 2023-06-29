use std::sync::OnceLock;

use atomic_take::AtomicTake;
use nsql_storage_engine::{ExecutionMode, ReadWriteExecutionMode, StorageEngine};

use crate::eval::{FunctionCatalog, ScalarFunction, TupleExpr};
use crate::table_storage::PrimaryKeyConflict;
use crate::tuple::Tuple;
use crate::{TableStorage, TableStorageInfo};

pub(crate) struct IndexStorage<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    storage: TableStorage<'env, 'txn, S, M>,
    index_expr: AtomicTake<TupleExpr>,
    prepared_expr: OnceLock<TupleExpr<Box<dyn ScalarFunction>>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> IndexStorage<'env, 'txn, S, M> {
    pub fn open(
        storage: &S,
        tx: M::TransactionRef<'txn>,
        info: IndexStorageInfo,
    ) -> Result<Self, S::Error> {
        let storage = TableStorage::open(storage, tx, info.table, vec![])?;
        Ok(Self {
            storage,
            index_expr: AtomicTake::new(info.index_expr),
            prepared_expr: OnceLock::new(),
        })
    }
}

impl<'env, 'txn, S: StorageEngine> IndexStorage<'env, 'txn, S, ReadWriteExecutionMode> {
    #[inline]
    pub fn insert(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S>,
        tx: &S::WriteTransaction<'env>,
        tuple: &Tuple,
    ) -> Result<(), anyhow::Error> {
        let expr = self
            .prepared_expr
            .get_or_try_init(|| self.index_expr.take().unwrap().prepare(catalog, tx))?;

        let tuple = expr.execute(tuple);
        self.storage
            .insert(catalog, tx, &tuple)?
            .map_err(|PrimaryKeyConflict { key }| anyhow::anyhow!("unique index conflict: {key}"))
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
