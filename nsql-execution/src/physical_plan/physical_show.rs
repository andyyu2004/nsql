use nsql_catalog::{Container, Namespace, Table};
use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    show: ir::ObjectType,
}

impl PhysicalShow {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        show: ir::ObjectType,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { show })
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
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let iter =
            catalog.all::<Namespace<S>>(tx).into_iter().flat_map(move |namespace| {
                match self.show {
                    ir::ObjectType::Table => namespace
                        .all::<Table<S>>(tx)
                        .into_iter()
                        .map(|table| Ok(Tuple::from(vec![Value::Text(table.name().to_string())]))),
                }
            });

        Ok(Box::new(fallible_iterator::convert(iter)))
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalShow {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "show {}s", self.show)?;
        Ok(())
    }
}
