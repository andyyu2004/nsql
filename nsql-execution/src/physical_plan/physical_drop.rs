use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use nsql_catalog::EntityRef;

use super::*;
use crate::Chunk;

#[derive(Debug)]
pub struct PhysicalDrop {
    refs: Vec<ir::EntityRef>,
    finished: AtomicBool,
}

impl PhysicalDrop {
    pub(crate) fn plan(refs: Vec<ir::EntityRef>) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { refs, finished: Default::default() })
    }
}

impl PhysicalNode for PhysicalDrop {
    fn desc(&self) -> &'static str {
        "drop"
    }

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
impl PhysicalSource for PhysicalDrop {
    fn estimated_cardinality(&self) -> usize {
        0
    }

    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        if self.finished.swap(true, atomic::Ordering::AcqRel) {
            return Ok(Chunk::empty());
        }

        let tx = ctx.tx();
        let catalog = ctx.catalog();
        for entity_ref in &self.refs {
            match entity_ref {
                ir::EntityRef::Table(table_ref) => table_ref.delete(&catalog, &tx)?,
            }
        }

        Ok(Chunk::empty())
    }
}
