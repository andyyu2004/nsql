use std::sync::Arc;

use nsql_catalog::{Container, Namespace};

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    show: ir::Show,
}

impl PhysicalShow {
    pub(crate) fn plan(show: ir::Show) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { show })
    }
}

impl PhysicalNode for PhysicalShow {
    fn desc(&self) -> &'static str {
        "create table"
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
impl PhysicalSource for PhysicalShow {
    fn estimated_cardinality(&self) -> usize {
        todo!()
    }

    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let namespaces = catalog.all::<Namespace>(&tx);

        match self.show {
            ir::Show::Tables => todo!(),
        }
    }
}
