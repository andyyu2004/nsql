use nsql_catalog::EntityRef;

use super::*;
use crate::ReadWriteExecutionMode;

pub struct PhysicalDrop<S> {
    refs: Vec<ir::EntityRef<S>>,
}

impl<S> fmt::Debug for PhysicalDrop<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalDrop").field("refs", &self.refs).finish()
    }
}

impl<S: StorageEngine> PhysicalDrop<S> {
    pub(crate) fn plan(
        refs: Vec<ir::EntityRef<S>>,
    ) -> Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>> {
        Arc::new(Self { refs })
    }
}

impl<S: StorageEngine> PhysicalNode<S, ReadWriteExecutionMode<S>> for PhysicalDrop<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S, ReadWriteExecutionMode<S>> for PhysicalDrop<S> {
    fn source(
        &self,
        ctx: &ExecutionContext<'_, '_, S, ReadWriteExecutionMode<S>>,
    ) -> ExecutionResult<SourceState<Chunk>> {
        let mut tx = ctx.tx_mut();
        let catalog = ctx.catalog();
        for entity_ref in &self.refs {
            match entity_ref {
                ir::EntityRef::Table(table_ref) => table_ref.delete(&catalog, &mut tx)?,
            }
        }

        Ok(SourceState::Done)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalDrop<S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
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
