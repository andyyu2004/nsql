use std::marker::PhantomData;

use super::*;

#[derive(Debug)]
pub struct PhysicalProjection<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    projection: ExecutableTupleExpr<'env, 'txn, S, M>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalProjection<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        projection: ExecutableTupleExpr<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalProjection<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, ecx, input))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        let output = self.projection.eval(&mut self.evaluator, storage, tx, &input)?;
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalProjection<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.projection.width()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalProjection<'env, 'txn, S, M>
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
