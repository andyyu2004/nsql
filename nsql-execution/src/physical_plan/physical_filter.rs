use super::*;

#[derive(Debug)]
pub struct PhysicalFilter {
    children: [Arc<dyn PhysicalNode>; 1],
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl PhysicalFilter {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], predicate })
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for PhysicalFilter {
    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let value = self.evaluator.evaluate_expr(&input, &self.predicate);
        // A null predicate is treated as false.
        match value.cast::<bool>(false).expect("this should have failed during planning") {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(input)),
        }
    }
}

impl PhysicalNode for PhysicalFilter {
    fn desc(&self) -> &'static str {
        "filter"
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
