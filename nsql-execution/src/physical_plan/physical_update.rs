use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use parking_lot::RwLock;

use super::*;

#[derive(Debug)]
pub(crate) struct PhysicalUpdate {
    children: [Arc<dyn PhysicalNode>; 1],
    table_ref: ir::TableRef,
    assignments: Box<[ir::Assignment]>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl PhysicalUpdate {
    pub fn plan(
        table_ref: ir::TableRef,
        source: Arc<dyn PhysicalNode>,
        assignments: Box<[ir::Assignment]>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self {
            table_ref,
            returning,
            assignments,
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
        })
    }
}

impl PhysicalNode for PhysicalUpdate {
    fn desc(&self) -> &'static str {
        "update"
    }

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
impl PhysicalSink for PhysicalUpdate {
    async fn sink(&self, ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &tx)?;
        let storage = table.storage();

        let (tuple, tid) = tuple.split_last().expect("expected tuple to be non-empty");

        // We expect the tid in the rightmost column of the tuple.
        let tid = match tid {
            ir::Value::Tid(tid) => tid,
            _ => unreachable!(),
        };

        storage.update(&tx, tid, &tuple).await.map_err(|report| report.into_error())?;

        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalUpdate {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        let returning = match &self.returning {
            Some(returning) => returning,
            None => return Ok(Chunk::empty()),
        };

        let tuple = match self.returning_tuples.write().pop_front() {
            Some(tuple) => tuple,
            None => return Ok(Chunk::empty()),
        };

        Ok(Chunk::singleton(self.returning_evaluator.evaluate(&tuple, returning)))
    }

    fn estimated_cardinality(&self) -> usize {
        if self.returning.is_some() { todo!() } else { 0 }
    }
}
