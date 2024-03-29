use std::marker::PhantomData;

use ir::Value;
use nsql_catalog::Namespace;
use nsql_storage_engine::{fallible_iterator, FallibleIterator};

use super::*;

#[derive(Debug)]
pub struct PhysicalShow<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    object_type: ir::ObjectType,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalShow<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        object_type: ir::ObjectType,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, object_type, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalShow<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalShow<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let tx = ecx.tcx();
        let catalog = ecx.catalog();

        let rows = match self.object_type {
            ir::ObjectType::Table => catalog
                .tables(tx)?
                .as_ref()
                .scan(..)?
                .filter(|table| Ok(table.namespace() == Namespace::MAIN))
                .map(move |table| Ok(T::from_iter([Value::Text(table.name().into())])))
                .collect::<Vec<_>>()?,
        };

        Ok(Box::new(fallible_iterator::convert(rows.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalShow<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "show {}s", self.object_type)?;
        Ok(())
    }
}
