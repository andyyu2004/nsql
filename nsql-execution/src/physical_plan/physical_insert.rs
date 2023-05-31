use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;
use crate::ReadWriteExecutionMode;

pub(crate) struct PhysicalInsert<'env, S> {
    children: [Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>; 1],
    table_ref: ir::TableRef<S>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<'env, S: StorageEngine> PhysicalInsert<'env, S> {
    pub fn plan(
        table_ref: ir::TableRef<S>,
        source: Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>> {
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
    for PhysicalInsert<'env, S>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

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
    for PhysicalInsert<'env, S>
{
    fn sink<'txn>(
        &self,
        ctx: <ReadWriteExecutionMode<S> as ExecutionMode<'env, S>>::Ref<
            'txn,
            ExecutionContext<'env, S, ReadWriteExecutionMode<S>>,
        >,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let catalog = ctx.catalog();
        let mut tx = ctx.tx_mut();
        let table = self.table_ref.get(&catalog, &**tx);
        // let storage = table.storage();
        // storage.append(&mut tx, &tuple)?;

        // FIXME just do the return evaluation here
        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

impl<'env, S: StorageEngine> PhysicalSource<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalInsert<'env, S>
{
    fn source<'txn>(
        self: Arc<Self>,
        ctx: <ReadWriteExecutionMode<S> as ExecutionMode<'env, S>>::Ref<
            'txn,
            ExecutionContext<'env, S, ReadWriteExecutionMode<S>>,
        >,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let returning = match &self.returning {
            Some(returning) => returning,
            None => return Ok(Box::new(fallible_iterator::empty())),
        };
        todo!();

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

impl<'env, S: StorageEngine> Explain<S> for PhysicalInsert<'env, S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "insert into {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
