use std::sync::Arc;

use nsql_catalog::{Container, CreateNamespaceInfo, Namespace};

use super::*;
use crate::Error;

#[derive(Debug)]
pub struct PhysicalCreateNamespace {
    info: ir::CreateNamespaceInfo,
}

impl PhysicalCreateNamespace {
    pub(crate) fn plan(info: ir::CreateNamespaceInfo) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { info })
    }
}

impl PhysicalNode for PhysicalCreateNamespace {
    fn desc(&self) -> &'static str {
        "create namespace"
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
impl PhysicalSource for PhysicalCreateNamespace {
    fn estimated_cardinality(&self) -> usize {
        0
    }

    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Option<Tuple>> {
        let tx = ctx.tx();
        let info = CreateNamespaceInfo { name: self.info.name.clone() };

        if let Err(err) = ctx.catalog.create::<Namespace>(&tx, info) {
            if !self.info.if_not_exists {
                return Err(Error::Catalog(err.into()))?;
            }
        }

        Ok(None)
    }
}
