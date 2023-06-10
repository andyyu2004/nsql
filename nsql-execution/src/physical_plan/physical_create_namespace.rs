use std::fmt;

use nsql_catalog::{CreateNamespaceInfo, Namespace};
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

#[derive(Debug)]
pub struct PhysicalCreateNamespace {
    info: CreateNamespaceInfo,
}

impl PhysicalCreateNamespace {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine>(
        info: CreateNamespaceInfo,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self { info })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalCreateNamespace
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalCreateNamespace
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx()?;
        let namespaces = catalog.system_table_write::<Namespace>(tx)?;
        let info = self.info.clone();

        namespaces.insert(Namespace::new(info.name.clone()));

        if let Err(err) = catalog.create::<Namespace<S>>(tx, info) {
            if !self.info.if_not_exists {
                return Err(err)?;
            }
        }

        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalCreateNamespace {
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "create namespace {}", self.info.name)?;
        Ok(())
    }
}
