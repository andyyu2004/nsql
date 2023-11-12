use std::marker::PhantomData;
use std::{cmp, mem};

use itertools::Itertools;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalOrder<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    ordering: Box<[ir::OrderExpr<ExecutableExpr<'env, 'txn, S, M>>]>,
    tuples: Vec<T>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalOrder<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        ordering: Box<[ir::OrderExpr<ExecutableExpr<'env, 'txn, S, M>>]>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalOrder<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let tuples = mem::take(&mut self.tuples);
        Ok(Box::new(fallible_iterator::convert(tuples.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalOrder<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }

    fn finalize(&mut self, ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>) -> ExecutionResult<()> {
        // sort tuples when the sink is finalized
        let tuples: &mut [T] = &mut self.tuples;
        let ordering = &self.ordering;

        let storage = ecx.storage();
        let prof = ecx.profiler();
        let tx = ecx.tcx();

        // FIXME can't use rayon's parallel sort as tx is not necessarily Sync
        tuples.sort_unstable_by(|a, b| {
            for order in ordering.iter() {
                // todo need a way to propogate error
                let a = order
                    .expr
                    .eval(&mut self.evaluator, storage, prof, tx, a)
                    .expect("failed to execute order expression");
                let b = order
                    .expr
                    .eval(&mut self.evaluator, storage, prof, tx, b)
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalOrder<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalOrder<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "order by (")?;
        fmt::Display::fmt(&self.ordering.iter().format(", "), f)?;
        write!(f, ")")?;
        Ok(())
    }
}
