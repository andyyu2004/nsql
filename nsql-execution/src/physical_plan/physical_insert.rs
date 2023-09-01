use std::sync::OnceLock;

use nsql_catalog::Table;
use nsql_core::Oid;
use nsql_storage::{PrimaryKeyConflict, TableStorage};
use nsql_storage_engine::fallible_iterator;
use parking_lot::{Mutex, RwLock};

use super::*;
use crate::ReadWriteExecutionMode;

#[derive(Debug)]
pub(crate) struct PhysicalInsert<'env, 'txn, S: StorageEngine> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>; 1],
    table_oid: Oid<Table>,
    storage: OnceLock<Mutex<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>>>,
    table: OnceLock<Table>,
    returning: ExecutableTupleExpr<S>,
    returning_tuples: RwLock<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalInsert<'env, 'txn, S> {
    pub fn plan(
        table_oid: Oid<Table>,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
        returning: ExecutableTupleExpr<S>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self {
            table_oid,
            returning,
            children: [source],
            storage: Default::default(),
            table: Default::default(),
            returning_tuples: Default::default(),
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn sink(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let catalog = ecx.catalog();
        let tx = ecx.tx();

        let table = self.table.get_or_try_init(|| catalog.get(tx, self.table_oid))?;

        let storage = self.storage.get_or_try_init(|| {
            table.storage::<S, ReadWriteExecutionMode>(catalog, tx).map(Mutex::new)
        })?;

        let storage: &mut TableStorage<'env, 'txn, _, _> = &mut storage.lock();
        storage.insert(&catalog, tx, &tuple)?.map_err(|PrimaryKeyConflict { key }| {
            anyhow::anyhow!("duplicate key `{key}` violates unique constraint")
        })?;

        if !self.returning.is_empty() {
            self.returning_tuples.write().push(self.returning.execute(
                catalog.storage(),
                &tx,
                &tuple,
            )?);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let returning = std::mem::take(&mut *self.returning_tuples.write());
        Ok(Box::new(fallible_iterator::convert(returning.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, S> for PhysicalInsert<'env, 'txn, S> {
    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "insert into {}", catalog.table(tx, self.table_oid)?.name())?;
        Ok(())
    }
}
