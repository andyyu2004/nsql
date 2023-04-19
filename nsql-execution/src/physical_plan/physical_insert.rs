use super::*;

#[derive(Debug)]
pub(crate) struct PhysicalInsert {
    children: Vec<Arc<dyn PhysicalNode>>,
    table_ref: ir::TableRef,
    returning: Option<Vec<ir::Expr>>,
}

impl PhysicalInsert {
    pub fn plan(
        table_ref: ir::TableRef,
        source: Arc<dyn PhysicalNode>,
        returning: Option<Vec<ir::Expr>>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { table_ref, returning, children: vec![source] })
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
    async fn sink(&self, ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &tx)?;
        let storage = table.storage();
        storage.append(&tx, &tuple).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalInsert {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<Option<Tuple>> {
        let returning = match &self.returning {
            Some(returning) => returning,
            None => return Ok(None),
        };
        todo!()
    }

    fn estimated_cardinality(&self) -> usize {
        if self.returning.is_some() { todo!() } else { 0 }
    }
}
