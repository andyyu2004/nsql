use std::fmt;

use nsql_catalog::{Container, CreateNamespaceInfo, Namespace};
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

#[derive(Debug)]
pub struct PhysicalCreateNamespace {
    info: ir::CreateNamespaceInfo,
}

impl PhysicalCreateNamespace {
    pub(crate) fn plan<'env, 'txn, S: StorageEngine>(
        info: ir::CreateNamespaceInfo,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>> {
        Arc::new(Self { info })
    }
}

impl<'env, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>
    for PhysicalCreateNamespace
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }
}

impl<'env, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode<S>>
    for PhysicalCreateNamespace
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let info = CreateNamespaceInfo { name: self.info.name.clone() };

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
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "create namespace {}", self.info.name)?;
        Ok(())
    }
}
