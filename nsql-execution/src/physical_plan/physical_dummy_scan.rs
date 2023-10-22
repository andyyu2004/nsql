use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::TupleStream;

#[derive(Debug)]
pub struct PhysicalDummyScan<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    width: Option<usize>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalDummyScan<'env, 'txn, S, M> {
    /// If `Some(n)`, then output no tuples (with `width = n`), otherwise output a single empty tuple (width 0).
    pub(crate) fn plan(
        width: Option<usize>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, width, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalDummyScan<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        if self.width.is_some() {
            Ok(Box::new(fallible_iterator::empty()))
        } else {
            Ok(Box::new(fallible_iterator::once(Tuple::empty())))
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalDummyScan<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.width.unwrap_or(0)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalDummyScan<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
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
