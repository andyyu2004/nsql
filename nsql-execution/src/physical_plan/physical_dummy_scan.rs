use super::*;

#[derive(Debug)]
pub struct PhysicalDummyScan;

impl<S> PhysicalDummyScan {
    pub(crate) fn plan() -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl<S> PhysicalSource<S> for PhysicalDummyScan {
    async fn source(&self, _ctx: &ExecutionContext<S>) -> ExecutionResult<SourceState<Chunk>> {
        Ok(SourceState::Final(Chunk::singleton(Tuple::empty())))
    }
}

impl<S> PhysicalNode<S> for PhysicalDummyScan {
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

impl<S> Explain<S> for PhysicalDummyScan {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "dummy scan")?;
        Ok(())
    }
}
