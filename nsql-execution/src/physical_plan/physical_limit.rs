use std::sync::atomic::{self, AtomicU64};

use async_trait::async_trait;

use super::*;

#[derive(Debug)]
pub struct PhysicalLimit {
    children: Vec<Arc<dyn PhysicalNode>>,
    yielded: AtomicU64,
    limit: u64,
}

impl PhysicalLimit {
    pub(crate) fn plan(source: Arc<dyn PhysicalNode>, limit: u64) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { children: vec![source], limit, yielded: AtomicU64::new(0) })
    }
}

#[async_trait]
impl PhysicalOperator for PhysicalLimit {
    async fn execute(
        &self,
        _ctx: &ExecutionContext,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        if self.yielded.fetch_add(1, atomic::Ordering::AcqRel) >= self.limit {
            return Ok(OperatorState::Done);
        }

        Ok(OperatorState::Yield(input))
    }
}

impl PhysicalNode for PhysicalLimit {
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
