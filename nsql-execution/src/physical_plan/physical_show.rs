use nsql_catalog::{Namespace, Table};
use nsql_storage::value::Value;
use nsql_storage_engine::{fallible_iterator, FallibleIterator, TransactionConversionHack};

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    object_type: ir::ObjectType,
}

impl PhysicalShow {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        object_type: ir::ObjectType,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { object_type })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalShow
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalShow
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tx: M::TransactionRef<'txn> = ctx.tx()?;
        let tx: &'txn dyn Transaction<'env, S> = tx.dyn_ref();
        let catalog = ctx.catalog();

        let iter = match self.object_type {
            ir::ObjectType::Table => Arc::new(catalog.tables(tx.dyn_ref())?)
                .scan_arc()?
                .filter(|table| Ok(table.namespace() == Namespace::MAIN))
                .map(|table| Ok(Tuple::from(vec![Value::Text(table.name().to_string())]))),
        };

        Ok(Box::new(iter))
    }
}

impl<S: StorageEngine> Explain<'_, S> for PhysicalShow {
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
