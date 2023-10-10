use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::config::ExplainOutput;

pub struct PhysicalExplain<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    opts: ir::ExplainOptions,
    child: PhysicalNodeId<'env, 'txn, S, M>,
    logical_explain: String,
    physical_explain: String,
    pipeline_explain: String,
}

impl<S, M> fmt::Debug for PhysicalExplain<'_, '_, S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalExplain").finish_non_exhaustive()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalExplain<'env, 'txn, S, M>
{
    #[inline]
    pub(crate) fn plan(
        opts: ir::ExplainOptions,
        child: PhysicalNodeId<'env, 'txn, S, M>,
        logical_explain: impl Into<String>,
        physical_explain: impl Into<String>,
        pipeline_explain: impl Into<String>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                opts,
                child,
                logical_explain: logical_explain.into(),
                physical_explain: physical_explain.into(),
                pipeline_explain: pipeline_explain.into(),
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        if self.opts.analyze { std::slice::from_ref(&self.child) } else { &[] }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        _tuple: Tuple,
    ) -> ExecutionResult<()> {
        // drop any tuples as we don't really care
        // we could capture metrics here if we wanted about how many output rows etc
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let scx = ecx.scx();

        let logical_explain = self.logical_explain.clone();
        let physical_explain = self.physical_explain.clone();
        let pipeline_explain = self.pipeline_explain.clone();

        let explain_output = scx.config().explain_output();
        let stringified = match explain_output {
            ExplainOutput::Physical => physical_explain,
            ExplainOutput::Pipeline => pipeline_explain,
            ExplainOutput::Logical => logical_explain,
            ExplainOutput::All => {
                format!(
                    "Logical:\n{}\n\nPhysical:\n{}\n\nPipeline:\n{}",
                    logical_explain, physical_explain, pipeline_explain
                )
            }
        };

        Ok(Box::new(fallible_iterator::once(Tuple::from(vec![Value::Text(stringified)]))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        _f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
