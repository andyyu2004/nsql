use ir::Value;
use nsql_core::Name;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalSet<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    name: Name,
    value: Value,
    scope: ir::VariableScope,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSet<'env, 'txn, S, M> {
    pub(crate) fn plan(
        name: Name,
        value: Value,
        scope: ir::VariableScope,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| Box::new(Self { id, name, value, scope }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalSet<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let scx = ecx.scx();
        match self.scope {
            // TODO we should store global configuration in the catalog
            ir::VariableScope::Global => {
                anyhow::bail!("global variables are not supported yet (use `SET LOCAL x = y`)")
            }
            ir::VariableScope::Local => scx.config().set(self.name.as_str(), self.value.clone())?,
        }
        Ok(Box::new(fallible_iterator::once(Tuple::empty())))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalSet<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalSet<'env, 'txn, S, M>
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
        write!(f, "dummy scan")?;
        Ok(())
    }
}
