use ir::Value;
use nsql_core::Name;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalSet {
    name: Name,
    value: Value,
    scope: ir::VariableScope,
}

impl PhysicalSet {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        name: Name,
        value: Value,
        scope: ir::VariableScope,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { name, value, scope })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalSet
{
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
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
    for PhysicalSet
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'_, S> for PhysicalSet {
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
