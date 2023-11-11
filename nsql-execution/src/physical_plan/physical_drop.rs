use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

pub struct PhysicalDrop<'env, 'txn, S, T> {
    id: PhysicalNodeId,
    refs: Vec<ir::EntityRef>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode, T>>,
}

impl<'env, 'txn, S, T> fmt::Debug for PhysicalDrop<'env, 'txn, S, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDrop").field("refs", &self.refs).finish()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait> PhysicalDrop<'env, 'txn, S, T> {
    pub(crate) fn plan(
        refs: Vec<ir::EntityRef>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, refs, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode, T> for PhysicalDrop<'env, 'txn, S, T>
{
    impl_physical_node_conversions!(ReadWriteExecutionMode; source; not sink, operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode, T>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode, T> for PhysicalDrop<'env, 'txn, S, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
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

impl<'env: 'txn, 'txn, S: StorageEngine, T: TupleTrait>
    Explain<'env, 'txn, S, ReadWriteExecutionMode> for PhysicalDrop<'env, 'txn, S, T>
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
