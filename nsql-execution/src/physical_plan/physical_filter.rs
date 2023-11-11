use std::marker::PhantomData;

use super::*;

#[derive(Debug)]
pub struct PhysicalFilter<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    predicate: ExecutableExpr<'env, 'txn, S, M>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalFilter<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        predicate: ExecutableExpr<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                child: source,
                predicate,
                evaluator: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalFilter<'env, 'txn, S, M, T>
{
    #[tracing::instrument(level = "debug", skip(self, ecx, input))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        input: &mut T,
    ) -> ExecutionResult<OperatorState<T>> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        let value = self.predicate.eval(&mut self.evaluator, storage, tx, input)?;
        let keep = value
            .cast::<Option<bool>>()
            .expect("this should have failed during planning")
            .unwrap_or(false);
        tracing::debug!(%keep, %input, %self.predicate, "filtering tuple");
        // A null predicate is treated as false.
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalFilter<'env, 'txn, S, M, T>
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalFilter<'env, 'txn, S, M, T>
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
        write!(f, "filter ")?;
        fmt::Display::fmt(&self.predicate, f)?;
        Ok(())
    }
}
