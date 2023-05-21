use std::sync::atomic::{self, AtomicUsize};

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: ir::Values,
    index: AtomicUsize,
}

impl PhysicalValues {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        values: ir::Values,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(PhysicalValues { values, index: AtomicUsize::new(0) })
    }
}

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalValues
{
    fn source(&self, _ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
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

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalValues
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalValues {
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
