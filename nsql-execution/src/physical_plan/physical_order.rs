use std::{cmp, mem};

use itertools::Itertools;
use nsql_storage_engine::fallible_iterator;
use parking_lot::RwLock;

use super::*;

#[derive(Debug)]
pub struct PhysicalOrder<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    child: PhysicalNodeId<'env, 'txn, S, M>,
    ordering: Box<[ir::OrderExpr<ExecutableExpr<S>>]>,
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOrder<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId<'env, 'txn, S, M>,
        ordering: Box<[ir::OrderExpr<ExecutableExpr<S>>]>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| {
            Arc::new(Self { id, child: source, ordering, tuples: Default::default() })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn source(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let tuples = mem::take(&mut *self.tuples.write());
        Ok(Box::new(fallible_iterator::convert(tuples.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }

    fn finalize(&self, ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        // sort tuples when the sink is finalized
        let tuples: &mut [Tuple] = &mut self.tuples.write();
        let ordering = &self.ordering;

        let storage = ecx.storage();
        let tx = ecx.tx();

        // FIXME can't use rayon's parallel sort as tx is not necessarily Sync
        tuples.sort_unstable_by(|a, b| {
            for order in ordering.iter() {
                // todo need a way to propogate error
                let a = order
                    .expr
                    .execute(storage, &tx, a)
                    .expect("failed to execute order expression");
                let b = order
                    .expr
                    .execute(storage, &tx, b)
                    .expect("failed to execute order expression");
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
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        std::slice::from_ref(&self.child)
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
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "order by (")?;
        fmt::Display::fmt(&self.ordering.iter().format(", "), f)?;
        write!(f, ")")?;
        Ok(())
    }
}
