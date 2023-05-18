use std::sync::atomic::{self, AtomicUsize};

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: ir::Values,
    index: AtomicUsize,
}

impl PhysicalValues {
    pub(crate) fn plan(values: ir::Values) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(PhysicalValues { values, index: AtomicUsize::new(0) })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalValues {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        let index = self.index.fetch_add(1, atomic::Ordering::SeqCst);
        if index >= self.values.len() {
            return Ok(SourceState::Done);
        }

        let evaluator = Evaluator::new();
        let exprs = &self.values[index];
        let tuple = evaluator.evaluate(&Tuple::empty(), exprs);

        Ok(SourceState::Yield(Chunk::singleton(tuple)))
    }
}

impl PhysicalNode<S> for PhysicalValues {
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

impl Explain for PhysicalValues {
    fn explain(
        &self,
        _catalog: &Catalog,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "scan values")?;
        Ok(())
    }
}
