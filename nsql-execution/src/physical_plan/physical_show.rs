use std::sync::atomic::{self, AtomicBool};

use nsql_catalog::{Container, Namespace, Table};
use nsql_storage::value::Value;

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    show: ir::ObjectType,
    finished: AtomicBool,
}

impl PhysicalShow {
    pub(crate) fn plan(show: ir::ObjectType) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { show, finished: Default::default() })
    }
}

impl PhysicalNode for PhysicalShow {
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
impl PhysicalSource for PhysicalShow {
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        if self.finished.swap(true, atomic::Ordering::AcqRel) {
            return Ok(Chunk::empty());
        }

        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let mut tuples = vec![];
        let namespaces = catalog.all::<Namespace>(&tx)?;
        for (_, namespace) in namespaces {
            match self.show {
                ir::ObjectType::Table => {
                    for (_, table) in namespace.all::<Table>(&tx)? {
                        tuples.push(Tuple::from(vec![Value::Text(table.name().to_string())]));
                    }
                }
            }
        }

        Ok(Chunk::from(tuples))
    }
}

impl Explain for PhysicalShow {
    fn explain(
        &self,
        _catalog: &Catalog,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "show {}s", self.show)?;
        Ok(())
    }
}
