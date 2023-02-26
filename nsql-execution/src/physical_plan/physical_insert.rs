use nsql_catalog::{Container, Namespace, Oid, Table};
use nsql_ir::Expr;

use super::*;

#[derive(Debug)]
pub(crate) struct PhysicalInsert {
    children: Vec<Arc<dyn PhysicalNode>>,
    schema: Oid<Namespace>,
    table: Oid<Table>,
    returning: Option<Vec<Expr>>,
}

impl PhysicalInsert {
    pub fn make(
        schema: Oid<Namespace>,
        table: Oid<Table>,
        source: Arc<dyn PhysicalNode>,
        returning: Option<Vec<Expr>>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { schema, table, returning, children: vec![source] })
    }
}

impl PhysicalNode for PhysicalInsert {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &self.children
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSink for PhysicalInsert {
    async fn sink(&self, ctx: &ExecutionContext<'_>, tuple: Tuple) -> ExecutionResult<()> {
        let schema = ctx
            .catalog()
            .get::<Namespace>(ctx.tx(), self.schema)?
            .expect("schema not found during insert execution");

        let table = schema
            .get::<Table>(ctx.tx(), self.table)?
            .expect("table not found during insert execution");

        let storage = table.storage();
        storage.append(ctx.tx(), tuple).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalInsert {
    async fn source(&self, _ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        todo!()
    }

    fn estimated_cardinality(&self) -> usize {
        if self.returning.is_some() { todo!() } else { 0 }
    }
}
