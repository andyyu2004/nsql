use std::sync::atomic::{self, AtomicU64};

use super::*;

pub struct PhysicalLimit<S> {
    children: [Arc<dyn PhysicalNode<S>>; 1],
    yielded: AtomicU64,
    limit: u64,
}

impl<S: StorageEngine> PhysicalLimit<S> {
    pub(crate) fn plan(source: Arc<dyn PhysicalNode<S>>, limit: u64) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { children: [source], limit, yielded: AtomicU64::new(0) })
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalOperator<S> for PhysicalLimit<S> {
    async fn execute(
        &self,
        _ctx: &ExecutionContext<'_, S>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        if self.yielded.fetch_add(1, atomic::Ordering::AcqRel) >= self.limit {
            return Ok(OperatorState::Done);
        }

        Ok(OperatorState::Yield(input))
    }
}

impl<S: StorageEngine> PhysicalNode<S> for PhysicalLimit<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &self.children
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalLimit<S> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "limit ({})", self.limit)?;
        Ok(())
    }
}
