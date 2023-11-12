use std::marker::PhantomData;

use nsql_core::Name;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalCteScan<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    cte_name: Name,
    /// The cte node that produces the data for this scan
    cte: PhysicalNodeId,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalCteScan<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        cte_name: Name,
        cte: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, cte_name, cte, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalCteScan<'env, 'txn, S, M, T>
{
    #[tracing::instrument(skip(self, ecx))]
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        // the materialized ctes should be populated by the `PhysicalCte` node
        let tuples = ecx.get_materialized_cte_data(&self.cte_name);
        let mut i = 0;
        let iter = std::iter::from_fn(move || {
            tuples.get(i).map(|tuple| {
                i += 1;
                Ok(tuple.clone())
            })
        });
        Ok(Box::new(fallible_iterator::convert(iter)))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalCteScan<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.cte].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple> Explain<'env, 'txn, S, M>
    for PhysicalCteScan<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, '_, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "cte scan on {}", self.cte_name)?;
        Ok(())
    }
}
