use std::marker::PhantomData;

use nsql_core::Name;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalCteScan<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    cte_name: Name,
    /// The cte node that produces the data for this scan
    cte: PhysicalNodeId,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalCteScan<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        cte_name: Name,
        cte: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, cte_name, cte, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalCteScan<'env, 'txn, S, M>
{
    #[tracing::instrument(skip(self, ecx))]
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalCteScan<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.cte].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalCteScan<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "cte scan on {}", self.cte_name)?;
        Ok(())
    }
}
