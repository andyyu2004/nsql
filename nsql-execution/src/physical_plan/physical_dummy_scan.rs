use super::*;

#[derive(Debug)]
pub struct PhysicalDummyScan;

impl PhysicalDummyScan {
    pub(crate) fn plan() -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalDummyScan {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        Ok(SourceState::Final(Chunk::singleton(Tuple::empty())))
    }
}

impl PhysicalNode<S> for PhysicalDummyScan {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

impl Explain for PhysicalDummyScan {
    fn explain(
        &self,
        _catalog: &Catalog,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "dummy scan")?;
        Ok(())
    }
}
