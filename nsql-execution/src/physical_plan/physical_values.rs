use std::marker::PhantomData;

use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalValues<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    values: Box<[ExecutableTupleExpr<'env, 'txn, S, M>]>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalValues<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        values: Box<[ExecutableTupleExpr<'env, 'txn, S, M>]>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, values, evaluator: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalValues<'env, 'txn, S, M>
{
    fn source<'s>(
        &'s mut self,
        ecx: &'s ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'s>> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        let mut index = 0;
        let iter = fallible_iterator::from_fn(move || {
            if index >= self.values.len() {
                return Ok(None);
            }

            let exprs: &ExecutableTupleExpr<'env, 'txn, S, M> = &self.values[index];
            let tuple = exprs.eval(&mut self.evaluator, storage, tx, &Tuple::empty())?;
            index += 1;

            Ok(Some(tuple))
        });
        Ok(Box::new(iter))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalValues<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        debug_assert!(!self.values.is_empty());
        self.values[0].width()
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalValues<'env, 'txn, S, M>
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
