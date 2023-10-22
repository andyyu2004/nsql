use std::marker::PhantomData;

use nsql_catalog::Table;
use nsql_core::Oid;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::ReadWriteExecutionMode;

#[derive(Debug)]
pub(crate) struct PhysicalUpdate<'env, 'txn, S> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 1],
    table: Oid<Table>,
    tuples: Vec<Tuple>,
    returning: ExecutableTupleExpr<'env, S, ReadWriteExecutionMode>,
    returning_tuples: Vec<Tuple>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalUpdate<'env, 'txn, S> {
    pub fn plan(
        table: Oid<Table>,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: PhysicalNodeId,
        returning: ExecutableTupleExpr<'env, S, ReadWriteExecutionMode>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                table,
                returning,
                tuples: Default::default(),
                children: [source],
                returning_tuples: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    impl_physical_node_conversions!(ReadWriteExecutionMode; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>) -> usize {
        self.returning.width()
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    fn sink(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }

    fn finalize(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<()> {
        let tx = ecx.tx();
        let catalog = ecx.catalog();
        let table = catalog.table(tx, self.table)?;
        let mut storage = table.storage::<S, ReadWriteExecutionMode>(catalog, tx)?;

        for tuple in &self.tuples {
            // FIXME we need to detect whether or not we actually updated something before adding it
            // to the returning set
            storage.update(tuple)?;

            if !self.returning.is_empty() {
                self.returning_tuples.push(self.returning.execute(catalog.storage(), tx, tuple)?);
            }
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    fn source(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let returning = std::mem::take(&mut self.returning_tuples);
        Ok(Box::new(fallible_iterator::convert(returning.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, S> for PhysicalUpdate<'env, 'txn, S> {
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "update {}", catalog.table(tx, self.table)?.name())?;
        Ok(())
    }
}
