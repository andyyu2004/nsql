use std::marker::PhantomData;

use anyhow::bail;
use ir::Value;
use nsql_core::Name;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalSet<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    name: Name,
    value: Value,
    scope: ir::VariableScope,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalSet<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        name: Name,
        value: Value,
        scope: ir::VariableScope,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, name, value, scope, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalSet<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let scx = ecx.scx();
        match self.scope {
            ir::VariableScope::Global => {
                scx.config().set(self.name.as_str(), self.value.clone())?
            }
            ir::VariableScope::Local => bail!("transaction local variables not supported yet"),
        }
        Ok(Box::new(fallible_iterator::once(T::empty())))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalSet<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    Explain<'env, 'txn, S, M> for PhysicalSet<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "dummy scan")?;
        Ok(())
    }
}
