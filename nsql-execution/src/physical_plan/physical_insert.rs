use std::collections::VecDeque;

use nsql_catalog::EntityRef;
use parking_lot::RwLock;

use super::*;

pub(crate) struct PhysicalInsert<S> {
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
    table_ref: ir::TableRef<S>,
    returning: Option<Box<[ir::Expr]>>,
    returning_tuples: RwLock<VecDeque<Tuple>>,
    returning_evaluator: Evaluator,
}

impl<S: StorageEngine> PhysicalInsert<S> {
    pub fn plan(
        table_ref: ir::TableRef<S>,
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

impl<S: StorageEngine> PhysicalNode<S, M> for PhysicalInsert<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSink<S, M> for PhysicalInsert<S> {
    fn sink(&self, ctx: &ExecutionContext<'_, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        let tx = ctx.tx();
        let table = self.table_ref.get(&ctx.catalog(), &tx);
        let storage = table.storage();
        storage.append(&tx, &tuple)?;

        // FIXME just do the return evaluation here
        if self.returning.is_some() {
            self.returning_tuples.write().push_back(tuple);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S, M> for PhysicalInsert<S> {
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

impl<S: StorageEngine> Explain<S> for PhysicalInsert<S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "insert into {}", self.table_ref.get(catalog, tx).name())?;
        Ok(())
    }
}
