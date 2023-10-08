use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

pub struct PhysicalDrop<'env, 'txn, S> {
    id: PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode>,
    refs: Vec<ir::EntityRef>,
}

impl<'env, 'txn, S> fmt::Debug for PhysicalDrop<'env, 'txn, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDrop").field("refs", &self.refs).finish()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalDrop<'env, 'txn, S> {
    pub(crate) fn plan(
        refs: Vec<ir::EntityRef>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode> {
        arena.alloc_with(|id| Arc::new(Self { id, refs }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalDrop<'env, 'txn, S>
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, ReadWriteExecutionMode>] {
        &[]
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
        Err(self)
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

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalDrop<'env, 'txn, S>
{
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        tracing::debug!("executing physical drop");

        let catalog = ecx.catalog();
        let tx = ecx.tx();
        for &entity_ref in &self.refs {
            tracing::debug!(entity = ?entity_ref, "dropping");
            match entity_ref {
                ir::EntityRef::Table(table) => {
                    catalog.drop_table(tx, table)?;
                }
            }
        }

        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, S> for PhysicalDrop<'env, 'txn, S> {
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "drop ")?;
        for (i, &entity_ref) in self.refs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            match entity_ref {
                ir::EntityRef::Table(table) => {
                    write!(f, "table {}", catalog.table(tx, table)?.name())?
                }
            }
        }

        Ok(())
    }
}
