use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use nsql_catalog::{Container, CreateTableInfo, Namespace, Oid, Table};
use nsql_core::schema::{Attribute, Schema};
use nsql_pager::Pager;
use nsql_storage::{TableStorage, TableStorageInfo};

use super::*;

#[derive(Debug)]
pub struct PhysicalCreateTable {
    finished: AtomicBool,
    namespace: Oid<Namespace>,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn make(
        pager: Arc<dyn Pager>,
        namespace: Oid<Namespace>,
        info: nsql_ir::CreateTableInfo,
    ) -> Arc<dyn PhysicalNode> {
        let attrs = info
            .columns
            .iter()
            .map(|c| Attribute::new(c.name.clone(), c.ty.clone()))
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(attrs));
        let info = CreateTableInfo {
            name: info.name,
            columns: info.columns,
            storage: Arc::new(TableStorage::new(TableStorageInfo { schema }, pager)),
        };
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

        let catalog = ctx.catalog();
        let schema = catalog
            .get::<Namespace>(ctx.tx(), self.namespace)?
            .expect("schema not found during execution");
        schema.create::<Table>(ctx.tx(), self.info.clone())?;

        Ok(None)
    }
}
