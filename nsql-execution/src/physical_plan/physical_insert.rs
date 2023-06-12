use nsql_catalog::TableRef;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;
use crate::ReadWriteExecutionMode;

pub(crate) struct PhysicalInsert<'env, 'txn, S> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>; 1],
    table_ref: TableRef,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<Vec<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalInsert<'env, 'txn, S> {
    pub fn plan(
        table_ref: TableRef,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self {
            table_ref,
            returning,
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
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
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let catalog = ctx.catalog();
        let tx = ctx.tx()?;
        let table = catalog.get(tx, self.table_ref.table)?;

        let mut storage = table.storage::<S, ReadWriteExecutionMode>(catalog, tx)?;
        storage.insert(&tuple)?;

        if let Some(return_expr) = &self.returning {
            self.returning_tuples
                .write()
                .push(self.returning_evaluator.evaluate(&tuple, return_expr));
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn source(
        self: Arc<Self>,
        _ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
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
        write!(f, "insert into {}", catalog.get(tx, self.table_ref.table)?.name())?;
        Ok(())
    }
}
