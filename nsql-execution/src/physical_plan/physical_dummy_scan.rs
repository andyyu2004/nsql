use super::*;

#[derive(Debug)]
pub struct PhysicalDummyScan;

impl PhysicalDummyScan {
    pub(crate) fn plan() -> Arc<dyn PhysicalNode> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalDummyScan {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        Ok(SourceState::Final(Chunk::singleton(Tuple::empty())))
    }
}

impl PhysicalNode for PhysicalDummyScan {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
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
