use super::*;

#[derive(Debug)]
pub struct PhysicalDummyScan;

impl PhysicalDummyScan {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>()
    -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalDummyScan
{
    fn source(&self, _ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        Ok(SourceState::Final(Chunk::singleton(Tuple::empty())))
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalDummyScan
{
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

impl<'env, S: StorageEngine> Explain<S> for PhysicalDummyScan {
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
