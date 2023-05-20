use std::sync::atomic::{self, AtomicUsize};

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: ir::Values,
    index: AtomicUsize,
}

impl PhysicalValues {
    pub(crate) fn plan<S: StorageEngine, M: ExecutionMode<S>>(
        values: ir::Values,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(PhysicalValues { values, index: AtomicUsize::new(0) })
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalSource<S, M> for PhysicalValues {
    fn source(&self, _ctx: &ExecutionContext<'_, S, M>) -> ExecutionResult<SourceState<Chunk>> {
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

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalValues {
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalValues {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "scan values")?;
        Ok(())
    }
}
