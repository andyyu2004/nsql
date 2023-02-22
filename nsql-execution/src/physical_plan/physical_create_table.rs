use std::sync::atomic::{self, AtomicBool};

use nsql_catalog::{Container, CreateTableInfo, Oid, Schema, Table};

use crate::{
    ExecutionContext, ExecutionResult, PhysicalNode, PhysicalNodeBase, PhysicalSource, Tuple,
};

#[derive(Debug)]
pub struct PhysicalCreateTable {
    finished: AtomicBool,
    schema: Oid<Schema>,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn make(schema: Oid<Schema>, info: CreateTableInfo) -> PhysicalNode {
        PhysicalNode::source(Self { finished: AtomicBool::new(false), schema, info })
    }
}

impl PhysicalNodeBase for PhysicalCreateTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn estimated_cardinality(&self) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNode] {
        &[]
    }
}

impl PhysicalSource for PhysicalCreateTable {
    fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Box<dyn Tuple>>> {
        if self.finished.load(atomic::Ordering::Relaxed) {
            return Ok(None);
        }

        let catalog = ctx.catalog();
        let schema = catalog
            .get::<Schema>(ctx.tx(), self.schema)?
            .expect("schema not found during execution");
        schema.create::<Table>(ctx.tx(), self.info.clone())?;

        Ok(None)
    }
}
