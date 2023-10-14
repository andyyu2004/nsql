use std::marker::PhantomData;

use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::TupleStream;

#[derive(Debug)]
pub struct PhysicalUnnest<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    expr: ExecutableExpr<S>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalUnnest<'env, 'txn, S, M> {
    pub(crate) fn plan(
        expr: ExecutableExpr<S>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, expr, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalUnnest<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        let values = match self.expr.execute(storage, &tx, &Tuple::empty())? {
            Value::Array(values) => values,
            Value::Null => Box::new([]),
            _ => panic!("unnest expression should evaluate to an array"),
        };

        Ok(Box::new(fallible_iterator::convert(
            values.into_vec().into_iter().map(|value| Tuple::from([value])).map(Ok),
        )))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUnnest<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalUnnest<'env, 'txn, S, M>
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
        write!(f, "unnest")?;
        Ok(())
    }
}
