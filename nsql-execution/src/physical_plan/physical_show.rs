use nsql_catalog::{Container, Namespace, Table};
use nsql_storage::value::Value;

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    show: ir::ObjectType,
}

impl PhysicalShow {
    pub(crate) fn plan<S: StorageEngine>(show: ir::ObjectType) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { show })
    }
}

impl<S: StorageEngine> PhysicalNode<S> for PhysicalShow {
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
impl<S: StorageEngine> PhysicalSource<S> for PhysicalShow {
    async fn source(&self, ctx: &ExecutionContext<S>) -> ExecutionResult<SourceState<Chunk>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let mut tuples = vec![];
        let namespaces = catalog.all::<Namespace<S>>(&tx);
        for (_, namespace) in namespaces {
            match self.show {
                ir::ObjectType::Table => {
                    for (_, table) in namespace.all::<Table<S>>(&tx) {
                        tuples.push(Tuple::from(vec![Value::Text(table.name().to_string())]));
                    }
                }
            }
        }

        Ok(SourceState::Final(Chunk::from(tuples)))
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalShow {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "show {}s", self.show)?;
        Ok(())
    }
}
