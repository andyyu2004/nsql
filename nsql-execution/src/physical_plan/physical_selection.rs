use nsql_storage::tuple::{Literal, Value};

use super::*;

#[derive(Debug)]
pub struct PhysicalSelection {
    children: Vec<Arc<dyn PhysicalNode>>,
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl PhysicalSelection {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { evaluator: Evaluator::new(), children: vec![source], predicate })
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for PhysicalSelection {
    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        match self.evaluator.evaluate_expr(&input, &self.predicate) {
            Value::Literal(Literal::Bool(b)) => match b {
                false => Ok(OperatorState::Continue),
                true => Ok(OperatorState::Yield(input)),
            },
            _ => unreachable!(),
        }
    }
}

impl PhysicalNode for PhysicalSelection {
    fn desc(&self) -> &'static str {
        "selection"
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
