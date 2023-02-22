use std::sync::atomic::{self, AtomicBool};

use nsql_catalog::{Container, CreateTableInfo, Oid, Schema, Table};

use crate::{ExecutionContext, ExecutionResult, PhysicalNode, PhysicalSource, Tuple};

#[derive(Debug)]
pub struct PhysicalCreateTable {
    finished: AtomicBool,
    schema: Oid<Schema>,
    info: CreateTableInfo,
}

impl PhysicalCreateTable {
    pub(crate) fn make(schema: Oid<Schema>, info: CreateTableInfo) -> Box<dyn PhysicalNode> {
        Box::new(Self { finished: AtomicBool::new(false), schema, info })
    }
}

impl PhysicalNode for PhysicalCreateTable {
    fn estimated_cardinality(&self) -> usize {
        0
    }
}

impl PhysicalSource for PhysicalCreateTable {
    fn source(
        &self,
        ctx: &dyn ExecutionContext,
        _out: Option<&mut dyn Tuple>,
    ) -> ExecutionResult<()> {
        if self.finished.load(atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let catalog = ctx.catalog();
        let schema = catalog
            .get::<Schema>(ctx.tx(), self.schema)?
            .expect("schema not found during execution");
        schema.create::<Table>(ctx.tx(), self.info.clone())?;

        Ok(())
    }
}
