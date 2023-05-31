use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;
use crate::ReadWriteExecutionMode;

pub(crate) struct PhysicalUpdate<'env, S> {
    children: [Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>; 1],
    table_ref: ir::TableRef<S>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<'env, S: StorageEngine> PhysicalUpdate<'env, S> {
    pub fn plan(
        table_ref: ir::TableRef<S>,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>> + 'env> {
        Arc::new(Self {
            table_ref,
            returning,
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
        })
    }
}

impl<'env, S: StorageEngine> PhysicalNode<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalUpdate<'env, S>
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>] {
        &self.children
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }
}

impl<'env, S: StorageEngine> PhysicalSink<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalUpdate<'env, S>
{
    fn sink<'txn>(
        &self,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode<S>>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &**tx);
        // let _storage = table.storage();

        let (_tuple, _tid) = tuple.split_last().expect("expected tuple to be non-empty");

        todo!();
        // storage.update(&tx, tid, &tuple).map_err(|report| report.into_error())?;

        // FIXReadWriteExecutionMode<S>E just do the return evaluation here
        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

impl<'env, S: StorageEngine> PhysicalSource<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalUpdate<'env, S>
{
    fn source<'txn>(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode<S>>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let returning = match &self.returning {
            Some(returning) => returning,
            None => return Ok(Box::new(fallible_iterator::empty())),
        };

        todo!()

        // let tuple = match self.returning_tuples.write().pop_front() {
        //     Some(tuple) => tuple,
        //     None => return Ok(SourceState::Done),
        // };
        //
        // Ok(SourceState::Yield(Chunk::singleton(
        //     self.returning_evaluator.evaluate(&tuple, returning),
        // )))
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalUpdate<'env, S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "update {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
