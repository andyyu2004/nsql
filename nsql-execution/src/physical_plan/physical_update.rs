use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use parking_lot::RwLock;

use super::*;

pub(crate) struct PhysicalUpdate<S> {
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
    table_ref: ir::TableRef<S>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<S: StorageEngine> PhysicalUpdate<S> {
    pub fn plan(
        table_ref: ir::TableRef<S>,
        // This is the source of the updates.
        // The schema should be that of the table being updated + the `tid` in the rightmost column
        source: Arc<dyn PhysicalNode<S, M>>,
        returning: Option<Box<[ir::Expr]>>,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self {
            table_ref,
            returning,
            children: [source],
            returning_tuples: Default::default(),
            returning_evaluator: Evaluator::new(),
        })
    }
}

impl<S: StorageEngine> PhysicalNode<S, M> for PhysicalUpdate<S> {
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &self.children
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSink<S, M> for PhysicalUpdate<S> {
    fn sink(&self, ctx: &ExecutionContext<'_, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &tx);
        let storage = table.storage();

        let (tuple, tid) = tuple.split_last().expect("expected tuple to be non-empty");

        todo!();
        // storage.update(&tx, tid, &tuple).map_err(|report| report.into_error())?;

        // FIXME just do the return evaluation here
        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S, M> for PhysicalUpdate<S> {
    #[inline]
    fn source(&self, _ctx: &ExecutionContext<'_, S, M>) -> ExecutionResult<SourceState<Chunk>> {
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

impl<S: StorageEngine> Explain<S> for PhysicalUpdate<S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "update {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
