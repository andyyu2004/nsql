use itertools::Itertools;

use super::*;

pub struct PhysicalProjection<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    projection: Box<[ir::Expr]>,
    evaluator: Evaluator,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalProjection<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        projection: Box<[ir::Expr]>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], projection })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalProjection<'env, 'txn, S, M>
{
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute(
        &self,
        _ctx: &'txn ExecutionContext<'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let output = self.evaluator.evaluate(&input, &self.projection);
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalProjection<'env, 'txn, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S>
    for PhysicalProjection<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: &Catalog,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "projection ({})", self.projection.iter().join(", "))?;
        Ok(())
    }
}
