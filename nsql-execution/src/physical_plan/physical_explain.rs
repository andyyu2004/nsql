use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::config::ExplainOutput;
use crate::executor::OutputSink;

pub struct PhysicalExplain<'env, 'txn, S, M> {
    opts: ir::ExplainOptions,
    child: PhysicalNodeId<'env, 'txn, S, M>,
    logical_plan_explain: String,
    physical_plan: PhysicalPlan<'env, 'txn, S, M>,
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
        logical_plan: impl Into<String>,
        physical_plan: PhysicalPlan<'env, 'txn, S, M>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            opts,
            logical_plan_explain: logical_plan.into(),
            child: physical_plan.root(),
            physical_plan,
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        if self.opts.analyze { std::slice::from_ref(&self.child) } else { &[] }
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn sink(
        &self,
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
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let scx = ecx.scx();
        let catalog = ecx.catalog();
        let tx = ecx.tx();
        let physical_plan = &self.physical_plan;

        let physical_explain = physical_plan.display(catalog, &tx).to_string();
        let pipeline = crate::build_pipelines(Arc::new(OutputSink::new()), physical_plan);
        let pipeline_explain = pipeline.display(catalog, &tx).to_string();

        let logical_explain = self.logical_plan_explain.clone();

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
