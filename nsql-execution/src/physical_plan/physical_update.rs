use nsql_catalog::Table;
use nsql_core::Oid;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;
use crate::ReadWriteExecutionMode;

#[derive(Debug)]
pub(crate) struct PhysicalUpdate<'env, 'txn, S> {
    children: [PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode>; 1],
    table: Oid<Table>,
    tuples: RwLock<Vec<Tuple>>,
    returning: ExecutableTupleExpr<S>,
    returning_tuples: RwLock<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalUpdate<'env, 'txn, S> {
    pub fn plan(
        table: Oid<Table>,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode>,
        returning: ExecutableTupleExpr<S>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self {
            table,
            returning,
            tuples: Default::default(),
            children: [source],
            returning_tuples: Default::default(),
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>) -> usize {
        self.returning.width()
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode>] {
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
        _ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }

    fn finalize(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<()> {
        let tx = ecx.tx();
        let catalog = ecx.catalog();
        let table = catalog.table(tx, self.table)?;
        let mut storage = table.storage::<S, ReadWriteExecutionMode>(catalog, tx)?;

        let tuples = self.tuples.read();
        for tuple in &*tuples {
            // FIXME we need to detect whether or not we actually updated something before adding it
            // to the returning set
            storage.update(tuple)?;

            if !self.returning.is_empty() {
                self.returning_tuples.write().push(self.returning.execute(
                    catalog.storage(),
                    tx,
                    tuple,
                )?);
            }
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalUpdate<'env, 'txn, S>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let returning = std::mem::take(&mut *self.returning_tuples.write());
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
