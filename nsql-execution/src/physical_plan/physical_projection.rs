use itertools::Itertools;

use super::*;

pub struct PhysicalProjection<'env, S, M> {
    children: [Arc<dyn PhysicalNode<'env, S, M>>; 1],
    projection: Box<[ir::Expr]>,
    evaluator: Evaluator,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalProjection<'env, S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, S, M>>,
        projection: Box<[ir::Expr]>,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], projection })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, S, M>
    for PhysicalProjection<'env, S, M>
{
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute<'txn>(
        &self,
        _ctx: M::Ref<'txn, ExecutionContext<'env, S, M>>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let output = self.evaluator.evaluate(&input, &self.projection);
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalProjection<'env, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S>
    for PhysicalProjection<'env, S, M>
{
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "projection ({})", self.projection.iter().join(", "))?;
        Ok(())
    }
}
