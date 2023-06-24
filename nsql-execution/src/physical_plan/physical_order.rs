use std::{cmp, mem};

use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;
use rayon::prelude::*;

use super::*;

#[derive(Debug)]
pub struct PhysicalOrder<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    ordering: Box<[ir::OrderExpr]>,
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOrder<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        ordering: Box<[ir::OrderExpr]>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { children: [source], ordering, tuples: Default::default() })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tuples = mem::take(&mut *self.tuples.write());
        Ok(Box::new(fallible_iterator::convert(tuples.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn sink(&self, _ecx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }

    fn finalize(&self, _ecx: &'txn ExecutionContext<'env, S, M>) -> ExecutionResult<()> {
        // sort tuples when the sink is finalized
        let tuples: &mut [Tuple] = &mut self.tuples.write();
        let ordering = &self.ordering;
        let evaluator = Evaluator::new();

        tuples.par_sort_unstable_by(|a, b| {
            for order in ordering.iter() {
                let a = evaluator.evaluate_expr(a, &order.expr);
                let b = evaluator.evaluate_expr(b, &order.expr);
                let cmp = a.partial_cmp(&b).unwrap();
                if cmp != cmp::Ordering::Equal {
                    return if order.asc { cmp } else { cmp.reverse() };
                }
            }

            cmp::Ordering::Equal
        });

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "order")?;
        Ok(())
    }
}
