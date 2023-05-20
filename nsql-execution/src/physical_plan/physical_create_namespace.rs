use std::fmt;

use nsql_catalog::{Container, CreateNamespaceInfo, Namespace};

use super::*;
use crate::{Chunk, ReadWriteExecutionMode};

#[derive(Debug)]
pub struct PhysicalCreateNamespace {
    info: ir::CreateNamespaceInfo,
}

impl PhysicalCreateNamespace {
    pub(crate) fn plan<S: StorageEngine>(
        info: ir::CreateNamespaceInfo,
    ) -> Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>> {
        Arc::new(Self { info })
    }
}

impl<S: StorageEngine> PhysicalNode<S, ReadWriteExecutionMode<S>> for PhysicalCreateNamespace {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S, ReadWriteExecutionMode<S>> for PhysicalCreateNamespace {
    fn source(
        &self,
        ctx: &ExecutionContext<'_, S, ReadWriteExecutionMode<S>>,
    ) -> ExecutionResult<SourceState<Chunk>> {
        let tx = ctx.tx();
        let info = CreateNamespaceInfo { name: self.info.name.clone() };

        if let Err(err) = ctx.catalog.create::<Namespace<S>>(&mut tx, info) {
            if !self.info.if_not_exists {
                return Err(err)?;
            }
        }

        Ok(SourceState::Done)
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
