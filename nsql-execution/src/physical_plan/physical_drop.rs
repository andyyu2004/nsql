use nsql_catalog::EntityRef;

use super::*;

#[derive(Debug)]
pub struct PhysicalDrop<S> {
    refs: Vec<ir::EntityRef<S>>,
}

impl<S> PhysicalDrop<S> {
    pub(crate) fn plan(refs: Vec<ir::EntityRef<S>>) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { refs })
    }
}

impl<S> PhysicalNode<S> for PhysicalDrop<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S> PhysicalSource<S> for PhysicalDrop<S> {
    async fn source(&self, ctx: &ExecutionContext<S>) -> ExecutionResult<SourceState<Chunk>> {
        let tx = ctx.tx();
        let catalog = ctx.catalog();
        for entity_ref in &self.refs {
            match entity_ref {
                ir::EntityRef::Table(table_ref) => table_ref.delete(&catalog, &tx)?,
            }
        }

        Ok(SourceState::Done)
    }
}

impl<S> Explain<S> for PhysicalDrop<S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "drop ")?;
        for (i, entity_ref) in self.refs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            match entity_ref {
                ir::EntityRef::Table(table_ref) => {
                    write!(f, "table {}", table_ref.get(catalog, tx).name())?
                }
            }
        }

        Ok(())
    }
}
