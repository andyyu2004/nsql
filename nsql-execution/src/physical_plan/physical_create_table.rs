use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use nsql_catalog::{Container, CreateTableInfo, Namespace, Oid, Table};
use nsql_core::schema::{Attribute, Schema};
use nsql_storage::{TableStorage, TableStorageInfo};

use super::*;

#[derive(Debug)]
pub struct PhysicalCreateTable {
    finished: AtomicBool,
    namespace: Oid<Namespace>,
    info: ir::CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn plan(
        namespace: Oid<Namespace>,
        info: ir::CreateTableInfo,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { finished: AtomicBool::new(false), namespace, info })
    }
}

impl PhysicalNode for PhysicalCreateTable {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalCreateTable {
    fn estimated_cardinality(&self) -> usize {
        0
    }

    async fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        if self.finished.load(atomic::Ordering::Relaxed) {
            return Ok(None);
        }

        let attrs = self
            .info
            .columns
            .iter()
            .map(|c| Attribute::new(c.name.clone(), c.ty.clone()))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(attrs));

        let info = CreateTableInfo {
            name: self.info.name.clone(),
            columns: self.info.columns.clone(),
            storage: Arc::new(
                TableStorage::initialize(ctx.pool(), TableStorageInfo::create(schema)).await?,
            ),
        };

        let catalog = ctx.catalog();
        let schema = catalog
            .get::<Namespace>(ctx.tx(), self.namespace)?
            .expect("schema not found during execution");
        schema.create::<Table>(ctx.tx(), info)?;

        Ok(None)
    }
}
