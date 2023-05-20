use atomic_take::AtomicTake;
use nsql_storage::value::Value;

use super::*;

pub struct PhysicalExplain<S, M> {
    stringified_plan: AtomicTake<String>,
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
}

impl<S, M> fmt::Debug for PhysicalExplain<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalExplain")
            .field("stringified_plan", &self.stringified_plan)
            .finish_non_exhaustive()
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalExplain<S, M> {
    #[inline]
    pub(crate) fn plan(
        stringified_plan: String,
        child: Arc<dyn PhysicalNode<S, M>>,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self { stringified_plan: AtomicTake::new(stringified_plan), children: [child] })
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalExplain<S, M> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        // no children as we don't actually need to run anything (unless we're doing an analyse which is not implemented)
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalSource<S, M> for PhysicalExplain<S, M> {
    fn source(&self, _ctx: &ExecutionContext<'_, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        let plan = self.stringified_plan.take().expect("should not be called again");
        Ok(SourceState::Final(Chunk::singleton(Tuple::from(vec![Value::Text(plan)]))))
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> Explain<S> for PhysicalExplain<S, M> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        _f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
