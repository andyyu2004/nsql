use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

pub struct PhysicalDrop<'env, 'txn, S> {
    id: PhysicalNodeId,
    refs: Vec<ir::EntityRef>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
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
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, refs, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalDrop<'env, 'txn, S>
{
    impl_physical_node_conversions!(ReadWriteExecutionMode; source; not sink, operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalDrop<'env, 'txn, S>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'_>> {
        tracing::debug!("executing physical drop");

        let catalog = ecx.catalog();
        let tx = ecx.tcx();
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

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalDrop<'env, 'txn, S>
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
