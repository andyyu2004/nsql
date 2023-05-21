use super::*;

#[derive(Debug)]
pub struct PhysicalDummyScan;

impl PhysicalDummyScan {
    pub(crate) fn plan<S: StorageEngine, M: ExecutionMode<S>>() -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalSource<S, M> for PhysicalDummyScan {
    fn source(&self, _ctx: &ExecutionContext<'_, '_, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        Ok(SourceState::Final(Chunk::singleton(Tuple::empty())))
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalDummyScan {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalDummyScan {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "dummy scan")?;
        Ok(())
    }
}
