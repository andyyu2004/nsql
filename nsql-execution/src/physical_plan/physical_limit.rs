use std::sync::atomic::{self, AtomicU64};

use super::*;

pub struct PhysicalLimit<S, M> {
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
    yielded: AtomicU64,
    limit: u64,
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalLimit<S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<S, M>>,
        limit: u64,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self { children: [source], limit, yielded: AtomicU64::new(0) })
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalOperator<S, M> for PhysicalLimit<S, M> {
    fn execute(
        &self,
        _ctx: &ExecutionContext<'_, '_, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        if self.yielded.fetch_add(1, atomic::Ordering::AcqRel) >= self.limit {
            return Ok(OperatorState::Done);
        }

        Ok(OperatorState::Yield(input))
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalLimit<S, M> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> Explain<S> for PhysicalLimit<S, M> {
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
