use std::marker::PhantomData;

use super::*;

#[derive(Debug)]
pub struct PhysicalProjection<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    projection: ExecutableTupleExpr<'env, 'txn, S, M>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalProjection<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        projection: ExecutableTupleExpr<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                child: source,
                projection,
                evaluator: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalProjection<'env, 'txn, S, M, T>
{
    #[tracing::instrument(level = "debug", skip(self, ecx, tuple))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: &mut T,
    ) -> ExecutionResult<OperatorState<T>> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        *tuple = self.projection.eval(&mut self.evaluator, storage, tx, tuple)?;
        tracing::debug!(%tuple, "evaluating projection");
        Ok(OperatorState::Yield)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalProjection<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        self.projection.width()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalProjection<'env, 'txn, S, M, T>
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
        write!(f, "projection (")?;
        fmt::Display::fmt(&self.projection, f)?;
        write!(f, ")")?;
        Ok(())
    }
}
