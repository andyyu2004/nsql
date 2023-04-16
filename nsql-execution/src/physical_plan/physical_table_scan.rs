use std::sync::OnceLock;

use nsql_catalog::{Container, Table};

use super::*;

#[derive(Debug)]
pub struct PhysicalTableScan {
    table_ref: ir::TableRef,
    table: OnceLock<Arc<Table>>,
}

impl PhysicalTableScan {
    pub(crate) fn plan(table_ref: ir::TableRef) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { table_ref, table: OnceLock::new() })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalTableScan {
    async fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        let table = self.table.get_or_try_init(|| {
            let namespace = ctx.catalog.get(ctx.tx, self.table_ref.namespace)?.unwrap();
            Ok::<_, nsql_catalog::Error>(namespace.get(ctx.tx, self.table_ref.table)?.unwrap())
        })?;

        let storage = table.storage();
        storage.scan(ctx.tx);

        todo!()
    }

    fn estimated_cardinality(&self) -> usize {
        todo!()
    }
}

impl PhysicalNode for PhysicalTableScan {
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
