use super::*;

#[derive(Debug)]
pub struct PhysicalProjection {
    children: Vec<Arc<dyn PhysicalNode>>,
    projections: Vec<ir::Expr>,
    evaluator: Evaluator,
}

impl PhysicalProjection {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode>,
        projections: Vec<ir::Expr>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { evaluator: Evaluator::new(), children: vec![source], projections })
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for PhysicalProjection {
    async fn execute(&self, _ctx: &ExecutionContext, input: Tuple) -> ExecutionResult<Tuple> {
        Ok(self.evaluator.evaluate(&input, &self.projections))
    }
}

impl PhysicalNode for PhysicalProjection {
    fn desc(&self) -> &'static str {
        "projection"
    }

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
