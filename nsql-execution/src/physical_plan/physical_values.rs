use std::sync::atomic::{self, AtomicUsize};

use nsql_ir::Values;

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: Values,
    index: AtomicUsize,
}

impl PhysicalValues {
    pub(crate) fn make(values: Values) -> Arc<dyn PhysicalNode> {
        Arc::new(PhysicalValues { values, index: AtomicUsize::new(0) })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalValues {
    async fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        let index = self.index.fetch_add(1, atomic::Ordering::SeqCst);
        if index >= self.values.len() {
            return Ok(None);
        }

        let evaluator = Evaluator::new();
        let exprs = &self.values[index];
        let tuple = evaluator.evaluate(exprs);

        Ok(Some(tuple))
    }

    fn estimated_cardinality(&self) -> usize {
        self.values.len()
    }
}

impl PhysicalNode for PhysicalValues {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }
}
