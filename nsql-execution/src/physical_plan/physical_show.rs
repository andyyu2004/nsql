use nsql_catalog::{Container, Namespace, Table};
use nsql_storage::value::Value;

use super::*;

#[derive(Debug)]
pub struct PhysicalShow {
    show: ir::ObjectType,
}

impl PhysicalShow {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        show: ir::ObjectType,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self { show })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M> for PhysicalShow {
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalShow
{
    fn source<'txn>(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let mut tuples = vec![];
        let namespaces = catalog.all::<Namespace<S>>(&**tx);
        for namespace in namespaces {
            match self.show {
                ir::ObjectType::Table => {
                    for table in namespace.all::<Table<S>>(&**tx) {
                        tuples.push(Tuple::from(vec![Value::Text(table.name().to_string())]));
                    }
                }
            }
        }

        todo!()
        // Ok(SourceState::Final(Chunk::from(tuples)))
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalShow {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "show {}s", self.show)?;
        Ok(())
    }
}
