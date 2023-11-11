use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalValues<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    values: Box<[ExecutableTupleExpr<'env, 'txn, S, M>]>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalValues<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        values: Box<[ExecutableTupleExpr<'env, 'txn, S, M>]>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, values, evaluator: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalValues<'env, 'txn, S, M, T>
{
    fn source<'s>(
        &'s mut self,
        ecx: &'s ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'s, T>> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        let mut index = 0;
        let iter = fallible_iterator::from_fn(move || {
            if index >= self.values.len() {
                return Ok(None);
            }

            let exprs: &ExecutableTupleExpr<'env, 'txn, S, M> = &self.values[index];
            let tuple = exprs.eval(&mut self.evaluator, storage, tx, &T::empty())?;
            index += 1;

            Ok(Some(tuple))
        });

        Ok(Box::new(iter))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalValues<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        debug_assert!(!self.values.is_empty());
        self.values[0].width()
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    Explain<'env, 'txn, S, M> for PhysicalValues<'env, 'txn, S, M, T>
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
        write!(f, "scan {} values", self.values.len())?;
        Ok(())
    }
}
