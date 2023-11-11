use std::marker::PhantomData;

use nsql_catalog::Table;
use nsql_core::Oid;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::ReadWriteExecutionMode;

#[derive(Debug)]
pub(crate) struct PhysicalUpdate<'env, 'txn, S, T> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 1],
    table: Oid<Table>,
    tuples: Vec<T>,
    returning: ExecutableTupleExpr<'env, 'txn, S, ReadWriteExecutionMode>,
    returning_tuples: Vec<T>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait> PhysicalUpdate<'env, 'txn, S, T> {
    pub fn plan(
        table: Oid<Table>,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: PhysicalNodeId,
        returning: ExecutableTupleExpr<'env, 'txn, S, ReadWriteExecutionMode>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                table,
                returning,
                tuples: Default::default(),
                children: [source],
                returning_tuples: Default::default(),
                evaluator: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode, T> for PhysicalUpdate<'env, 'txn, S, T>
{
    impl_physical_node_conversions!(ReadWriteExecutionMode; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode, T>) -> usize {
        self.returning.width()
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode, T> for PhysicalUpdate<'env, 'txn, S, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }

    fn finalize(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode, T>,
    ) -> ExecutionResult<()> {
        let tx = ecx.tcx();
        let catalog = ecx.catalog();
        let table = catalog.table(tx, self.table)?;
        let mut storage = table.storage::<S, ReadWriteExecutionMode>(catalog, tx)?;

        for tuple in &self.tuples {
            // FIXME we need to detect whether or not we actually updated something before adding it
            // to the returning set
            // FIXME more efficient conversion
            storage.update(&tuple.clone().into())?;

            if !self.returning.is_empty() {
                self.returning_tuples.push(self.returning.eval(
                    &mut self.evaluator,
                    catalog.storage(),
                    tx,
                    tuple,
                )?);
            }
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode, T> for PhysicalUpdate<'env, 'txn, S, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let returning = std::mem::take(&mut self.returning_tuples);
        Ok(Box::new(fallible_iterator::convert(returning.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    Explain<'env, 'txn, S, ReadWriteExecutionMode> for PhysicalUpdate<'env, 'txn, S, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, ReadWriteExecutionMode> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "update {}", catalog.table(tx, self.table)?.name())?;
        Ok(())
    }
}
