use std::sync::atomic::{self, AtomicBool};

use nsql_storage::value::Value;

use super::*;

#[derive(Debug)]
pub struct PhysicalExplain {
    node: Arc<dyn PhysicalNode>,
    finished: AtomicBool,
}

impl PhysicalExplain {
    pub(crate) fn plan(node: Arc<dyn PhysicalNode>) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { node, finished: Default::default() })
    }
}

impl PhysicalNode for PhysicalExplain {
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

#[async_trait::async_trait]
impl PhysicalSource for PhysicalExplain {
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        if self.finished.swap(true, atomic::Ordering::AcqRel) {
            return Ok(Chunk::empty());
        }

        let explained = explain::explain(ctx, self.node.as_ref());
        Ok(Chunk::singleton(Tuple::from(vec![Value::Text(explained.to_string())])))
    }
}

impl Explain for PhysicalExplain {
    fn explain(&self, _ctx: &ExecutionContext, _f: &mut fmt::Formatter<'_>) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
