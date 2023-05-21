use atomic_take::AtomicTake;
use nsql_storage::value::Value;

use super::*;

pub struct PhysicalExplain<'env, S, M> {
    stringified_plan: AtomicTake<String>,
    children: [Arc<dyn PhysicalNode<'env, S, M>>; 1],
}

impl<S, M> fmt::Debug for PhysicalExplain<'_, S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalExplain")
            .field("stringified_plan", &self.stringified_plan)
            .finish_non_exhaustive()
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalExplain<'env, S, M> {
    #[inline]
    pub(crate) fn plan(
        stringified_plan: String,
        child: Arc<dyn PhysicalNode<'env, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, S, M> + 'env> {
        Arc::new(Self { stringified_plan: AtomicTake::new(stringified_plan), children: [child] })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalExplain<'env, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        // no children as we don't actually need to run anything (unless we're doing an analyse which is not implemented)
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalExplain<'env, S, M>
{
    fn source(&self, _ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        let plan = self.stringified_plan.take().expect("should not be called again");
        Ok(SourceState::Final(Chunk::singleton(Tuple::from(vec![Value::Text(plan)]))))
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S> for PhysicalExplain<'env, S, M> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        _f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
