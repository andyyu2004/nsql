use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::TupleStream;

#[derive(Debug)]
pub struct PhysicalDummyScan<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    width: Option<usize>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalDummyScan<'env, 'txn, S, M, T>
{
    /// If `Some(n)`, then output no tuples (with `width = n`), otherwise output a single empty tuple (width 0).
    pub(crate) fn plan(
        width: Option<usize>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, width, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalDummyScan<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        if self.width.is_some() {
            Ok(Box::new(fallible_iterator::empty()))
        } else {
            Ok(Box::new(fallible_iterator::once(T::empty())))
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalDummyScan<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        self.width.unwrap_or(0)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    Explain<'env, 'txn, S, M> for PhysicalDummyScan<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        if self.width.is_some() {
            write!(f, "empty")?;
        } else {
            write!(f, "dummy scan")?;
        }
        Ok(())
    }
}
