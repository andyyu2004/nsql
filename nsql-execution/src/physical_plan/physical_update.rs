use nsql_catalog::TableRef;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;
use crate::ReadWriteExecutionMode;

pub(crate) struct PhysicalUpdate<'env, 'txn, S> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>; 1],
    table_ref: TableRef,
    tuples: RwLock<Vec<Tuple>>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<Vec<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalUpdate<'env, 'txn, S> {
    pub fn plan(
        table_ref: TableRef,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self {
            table_ref,
            returning,
            tuples: Default::default(),
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>] {
        &self.children
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    #[inline]
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
    for PhysicalUpdate<'env, 'txn, S>
{
    fn sink(
        &self,
        _ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }

    fn finalize(
        &self,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<()> {
        let tx = ctx.tx()?;
        todo!();
        // let table = self.table_ref.get(&ctx.catalog(), tx);
        // let mut storage = table.storage(ctx.storage(), tx)?;
        //
        // let tuples = self.tuples.read();
        // for tuple in &*tuples {
        //     // FIXME we need to detect whether or not we actually updated something before adding it
        //     // to the returning set
        //     storage.update(tuple)?;
        //
        //     if let Some(return_expr) = &self.returning {
        //         self.returning_tuples
        //             .write()
        //             .push(self.returning_evaluator.evaluate(tuple, return_expr));
        //     }
        // }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    fn source(
        self: Arc<Self>,
        _ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let returning = std::mem::take(&mut *self.returning_tuples.write());
        Ok(Box::new(fallible_iterator::convert(returning.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'_, S> for PhysicalUpdate<'env, 'txn, S> {
    fn explain(
        &self,
        catalog: Catalog<'_, S>,
        tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        todo!();
        // write!(f, "update {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
