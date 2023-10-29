use std::marker::PhantomData;
use std::{cmp, mem};

use itertools::Itertools;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalOrder<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    ordering: Box<[ir::OrderExpr<ExecutableExpr<'env, S, M>>]>,
    tuples: Vec<Tuple>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOrder<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        ordering: Box<[ir::OrderExpr<ExecutableExpr<'env, S, M>>]>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                child: source,
                ordering,
                tuples: Default::default(),
                evaluator: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let tuples = mem::take(&mut self.tuples);
        Ok(Box::new(fallible_iterator::convert(tuples.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }

    fn finalize(&mut self, ecx: &ExecutionContext<'_, 'env, 'txn, S, M>) -> ExecutionResult<()> {
        // sort tuples when the sink is finalized
        let tuples: &mut [Tuple] = &mut self.tuples;
        let ordering = &self.ordering;

        let storage = ecx.storage();
        let tx = ecx.tcx();

        // FIXME can't use rayon's parallel sort as tx is not necessarily Sync
        tuples.sort_unstable_by(|a, b| {
            for order in ordering.iter() {
                // todo need a way to propogate error
                let a = order
                    .expr
                    .eval(&mut self.evaluator, storage, tx, a)
                    .expect("failed to execute order expression");
                let b = order
                    .expr
                    .eval(&mut self.evaluator, storage, tx, b)
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
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalOrder<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "order by (")?;
        fmt::Display::fmt(&self.ordering.iter().format(", "), f)?;
        write!(f, ")")?;
        Ok(())
    }
}
