use nsql_catalog::Namespace;
use nsql_storage::value::Value;
use nsql_storage_engine::{FallibleIterator, TransactionConversionHack};

use super::*;

#[derive(Debug)]
pub struct PhysicalShow<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    object_type: ir::ObjectType,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalShow<'env, 'txn, S, M> {
    pub(crate) fn plan(
        object_type: ir::ObjectType,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| Arc::new(Self { id, object_type }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalShow<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalShow<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tx: M::TransactionRef<'txn> = ecx.tx();
        let tx: &'txn dyn Transaction<'env, S> = tx.dyn_ref();
        let catalog = ecx.catalog();

        let iter = match self.object_type {
            ir::ObjectType::Table => Arc::new(catalog.tables(tx.dyn_ref())?)
                .scan_arc()?
                .filter(|table| Ok(table.namespace() == Namespace::MAIN))
                .map(move |table| Ok(Tuple::from(vec![Value::Text(table.name().to_string())]))),
        };

        Ok(Box::new(iter))
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalShow<'env, '_, S, M>
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
        write!(f, "show {}s", self.object_type)?;
        Ok(())
    }
}
