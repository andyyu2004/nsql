use itertools::Itertools;

use super::*;

pub struct PhysicalProjection<S, M> {
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
    projection: Box<[ir::Expr]>,
    evaluator: Evaluator,
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalProjection<S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<S, M>>,
        projection: Box<[ir::Expr]>,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], projection })
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalOperator<S, M> for PhysicalProjection<S, M> {
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute(
        &self,
        _ctx: &ExecutionContext<'_, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let output = self.evaluator.evaluate(&input, &self.projection);
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalProjection<S, M> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> Explain<S> for PhysicalProjection<S, M> {
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
