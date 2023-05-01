use std::sync::Arc;

use nsql_catalog::{Column, Container, CreateTableInfo, Namespace, Table};
use nsql_core::schema::{Attribute, Schema};
use nsql_storage::{TableStorage, TableStorageInfo};

use super::*;
use crate::Chunk;

#[derive(Debug)]
pub struct PhysicalCreateTable {
    info: ir::CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn plan(info: ir::CreateTableInfo) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { info })
    }
}

impl PhysicalNode for PhysicalCreateTable {
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
impl PhysicalSource for PhysicalCreateTable {
    fn estimated_cardinality(&self) -> usize {
        0
    }

    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        let attrs = self
            .info
            .columns
            .iter()
            .map(|c| Attribute::new(c.name.clone(), c.ty.clone()))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(attrs));

        let info = CreateTableInfo {
            name: self.info.name.clone(),
            storage: Arc::new(
                TableStorage::initialize(ctx.pool(), TableStorageInfo::create(schema))
                    .await
                    .map_err(|report| report.into_error())?,
            ),
        };

        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let schema = catalog
            .get::<Namespace>(&tx, self.info.namespace)?
            .expect("schema not found during execution");

        let table_oid = schema.create::<Table>(&tx, info)?;
        let table = schema.get::<Table>(&tx, table_oid)?.unwrap();
        for info in &self.info.columns {
            table.create::<Column>(&tx, info.clone())?;
        }

        Ok(Chunk::empty())
    }
}
