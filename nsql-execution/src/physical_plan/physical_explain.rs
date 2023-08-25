use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::config::ExplainOutput;

pub struct PhysicalExplain<'env, 'txn, S, M> {
    logical_plan: Box<ir::Plan<opt::Query>>,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
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
        logical_plan: Box<ir::Plan<opt::Query>>,
        physical_plan: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { logical_plan, children: [physical_plan] })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        // no children as we don't actually need to run anything (unless we're doing an analyse which is not implemented)
        &[]
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
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
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
        let physical_plan = &self.children[0];

        let physical_explain =
            PhysicalPlan(Arc::clone(physical_plan)).display(catalog, &tx).to_string();

        let pipeline = crate::build_pipelines(
            Arc::new(OutputSink::default()),
            PhysicalPlan(Arc::clone(physical_plan)),
        );
        let pipeline_explain = pipeline.display(catalog, &tx).to_string();

        let logical_explain = self.logical_plan.to_string();

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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'_, S>
    for PhysicalExplain<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        _f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
