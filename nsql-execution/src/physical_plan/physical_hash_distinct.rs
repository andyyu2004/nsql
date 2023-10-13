use std::marker::PhantomData;

use rustc_hash::FxHashSet;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashDistinct<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    seen: FxHashSet<Tuple>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashDistinct<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        child: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, child, seen: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalHashDistinct<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, _ecx, tuple))]
    fn execute(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        // FIXME can avoid unnecessary tuple clones using `DashMap` raw-api
        let keep = self.seen.insert(tuple.clone());
        tracing::debug!(%keep, %tuple, "deduping tuple");
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(tuple)),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalHashDistinct<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalHashDistinct<'env, 'txn, S, M>
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
        write!(f, "hash distinct")?;
        Ok(())
    }
}
