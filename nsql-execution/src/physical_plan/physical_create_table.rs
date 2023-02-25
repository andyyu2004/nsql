use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use nsql_catalog::{Container, CreateTableInfo, Oid, Schema, Table};
use nsql_pager::Pager;
use nsql_storage::TableStorage;

use super::*;

#[derive(Debug)]
pub struct PhysicalCreateTable {
    finished: AtomicBool,
    schema: Oid<Schema>,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn make<P: Pager>(
        pager: Arc<P>,
        schema: Oid<Schema>,
        info: nsql_ir::CreateTableInfo,
    ) -> Arc<dyn PhysicalNode> {
        let info = CreateTableInfo {
            name: info.name,
            columns: info.columns,
            storage: Arc::new(TableStorage::new(pager)),
        };
        Arc::new(Self { finished: AtomicBool::new(false), schema, info })
    }
}

impl PhysicalNode for PhysicalCreateTable {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn crate::PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }
}

impl PhysicalSource for PhysicalCreateTable {
    fn estimated_cardinality(&self) -> usize {
        0
    }

    fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        if self.finished.load(atomic::Ordering::Relaxed) {
            return Ok(None);
        }

        let catalog = ctx.catalog();
        let schema = catalog
            .get::<Schema>(ctx.tx(), self.schema)?
            .expect("schema not found during execution");
        schema.create::<Table>(ctx.tx(), self.info.clone())?;

        Ok(None)
    }
}
