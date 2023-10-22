use std::marker::PhantomData;

use super::*;

#[derive(Debug)]
pub struct PhysicalFilter<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    predicate: ExecutableExpr<'env, S, M>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalFilter<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        predicate: ExecutableExpr<'env, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, child: source, predicate, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalFilter<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, ecx, input))]
    fn execute(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        let value = self.predicate.execute(storage, tx, &input)?;
        let keep = value
            .cast::<Option<bool>>()
            .expect("this should have failed during planning")
            .unwrap_or(false);
        tracing::debug!(%keep, %input, %self.predicate, "filtering tuple");
        // A null predicate is treated as false.
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(input)),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalFilter<'env, 'txn, S, M>
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
    for PhysicalFilter<'env, 'txn, S, M>
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
        write!(f, "filter ")?;
        fmt::Display::fmt(&self.predicate, f)?;
        Ok(())
    }
}
