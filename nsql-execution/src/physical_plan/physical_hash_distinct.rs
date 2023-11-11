use std::marker::PhantomData;

use rustc_hash::FxHashSet;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashDistinct<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    seen: FxHashSet<T>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalHashDistinct<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        child: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, child, seen: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalHashDistinct<'env, 'txn, S, M, T>
{
    #[tracing::instrument(level = "debug", skip(self, _ecx, tuple))]
    fn execute(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<OperatorState<T>> {
        // FIXME can avoid unnecessary tuple clones using `DashMap` raw-api
        let keep = self.seen.insert(tuple.clone());
        tracing::debug!(%keep, %tuple, "deduping tuple");
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(tuple)),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalHashDistinct<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.child].width(nodes)
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    Explain<'env, 'txn, S, M> for PhysicalHashDistinct<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "hash distinct")?;
        Ok(())
    }
}
