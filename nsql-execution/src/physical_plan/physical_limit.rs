use std::sync::atomic::{self, AtomicU64};

use super::*;

pub struct PhysicalLimit<'env, S, M> {
    children: [Arc<dyn PhysicalNode<'env, S, M>>; 1],
    yielded: AtomicU64,
    limit: u64,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalLimit<'env, S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, S, M>>,
        limit: u64,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self { children: [source], limit, yielded: AtomicU64::new(0) })
    }
}

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, S, M>
    for PhysicalLimit<'env, S, M>
{
    fn execute(
        &self,
        _ctx: &ExecutionContext<'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        if self.yielded.fetch_add(1, atomic::Ordering::AcqRel) >= self.limit {
            return Ok(OperatorState::Done);
        }

        Ok(OperatorState::Yield(input))
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalLimit<'env, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S> for PhysicalLimit<'env, S, M> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "limit ({})", self.limit)?;
        Ok(())
    }
}
