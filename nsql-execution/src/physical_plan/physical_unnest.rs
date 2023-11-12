use std::marker::PhantomData;

use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::TupleStream;

#[derive(Debug)]
pub struct PhysicalUnnest<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    expr: ExecutableExpr<'env, 'txn, S, M>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalUnnest<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        expr: ExecutableExpr<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, expr, evaluator: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalUnnest<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let storage = ecx.storage();
        let prof = ecx.profiler();
        let tcx = ecx.tcx();

        let values =
            match self.expr.eval(&mut self.evaluator, storage, prof, tcx, &FlatTuple::empty())? {
                Value::Array(values) => values,
                Value::Null => Box::new([]),
                _ => panic!("unnest expression should evaluate to an array"),
            };

        Ok(Box::new(fallible_iterator::convert(
            values.into_vec().into_iter().map(|value| T::from_iter([value])).map(Ok),
        )))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalUnnest<'env, 'txn, S, M, T>
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
    Explain<'env, 'txn, S, M> for PhysicalUnnest<'env, 'txn, S, M, T>
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
        write!(f, "unnest")?;
        Ok(())
    }
}
