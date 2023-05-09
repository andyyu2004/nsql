use nsql_catalog::EntityRef;

use super::*;

#[derive(Debug)]
pub struct PhysicalDrop {
    refs: Vec<ir::EntityRef>,
}

impl PhysicalDrop {
    pub(crate) fn plan(refs: Vec<ir::EntityRef>) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { refs })
    }
}

impl PhysicalNode for PhysicalDrop {
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
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
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

impl Explain for PhysicalDrop {
    fn explain(&self, ctx: &ExecutionContext, f: &mut fmt::Formatter<'_>) -> explain::Result {
        write!(f, "drop ")?;
        for (i, entity_ref) in self.refs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            match entity_ref {
                ir::EntityRef::Table(table_ref) => {
                    write!(f, "table {}", table_ref.get(&ctx.catalog, &ctx.tx).name())?
                }
            }
        }

        Ok(())
    }
}
