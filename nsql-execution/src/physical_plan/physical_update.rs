use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use parking_lot::RwLock;

use super::*;

#[derive(Debug)]
pub(crate) struct PhysicalUpdate {
    children: [Arc<dyn PhysicalNode<S>>; 1],
    table_ref: ir::TableRef,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl PhysicalUpdate {
    pub fn plan(
        table_ref: ir::TableRef,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: Arc<dyn PhysicalNode<S>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self {
            table_ref,
            returning,
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
        })
    }
}

impl PhysicalNode<S> for PhysicalUpdate {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &self.children
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSink for PhysicalUpdate {
    async fn sink(&self, ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &tx);
        let storage = table.storage();

        let (tuple, tid) = tuple.split_last().expect("expected tuple to be non-empty");

        // We expect the tid in the rightmost column of the tuple.
        let tid = match tid {
            ir::Value::Tid(tid) => tid,
            val => unreachable!("expected tid to be in the rightmost column, got {}", val.ty()),
        };

        storage.update(&tx, tid, &tuple).await.map_err(|report| report.into_error())?;

        // FIXME just do the return evaluation here
        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalUpdate {
    #[inline]
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        let returning = match &self.returning {
            Some(returning) => returning,
            None => return Ok(SourceState::Done),
        };

        let tuple = match self.returning_tuples.write().pop_front() {
            Some(tuple) => tuple,
            None => return Ok(SourceState::Done),
        };

        Ok(SourceState::Yield(Chunk::singleton(
            self.returning_evaluator.evaluate(&tuple, returning),
        )))
    }
}

impl Explain for PhysicalUpdate {
    fn explain(
        &self,
        catalog: &Catalog,
        tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "update {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
