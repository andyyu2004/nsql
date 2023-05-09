use super::*;

#[derive(Debug)]
pub struct PhysicalProjection {
    children: [Arc<dyn PhysicalNode>; 1],
    projection: Box<[ir::Expr]>,
    evaluator: Evaluator,
}

impl PhysicalProjection {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode>,
        projection: Box<[ir::Expr]>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], projection })
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for PhysicalProjection {
    #[tracing::instrument(skip(self, _ctx, input))]
    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let output = self.evaluator.evaluate(&input, &self.projection);
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl PhysicalNode for PhysicalProjection {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &self.children
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }
}

impl Explain for PhysicalProjection {
    fn explain(&self, _ctx: &ExecutionContext, f: &mut fmt::Formatter<'_>) -> explain::Result {
        write!(f, "projection")?;
        Ok(())
    }
}
