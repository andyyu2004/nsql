use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

use nsql_catalog::{Container, Entity, Namespace, Table};
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
impl PhysicalSource for PhysicalShow {
    fn estimated_cardinality(&self) -> usize {
        todo!()
    }

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
                        tuples.push(Tuple::new(
                            vec![Value::Text(table.name().to_string())].into_boxed_slice(),
                        ));
                    }
                }
            }
        }

        Ok(Chunk::from(tuples))
    }
}
