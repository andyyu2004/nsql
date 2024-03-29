use std::marker::PhantomData;

use nsql_arena::ArenaMap;
use nsql_storage::value::Value;
use nsql_storage_engine::fallible_iterator;

use super::explain::ExplainTree;
use super::*;
use crate::analyze::AnalyzeMode;
use crate::config::ExplainOutput;

pub struct PhysicalExplain<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    opts: ir::ExplainOptions,
    child: PhysicalNodeId,
    logical_explain: Arc<str>,
    physical_explain: ExplainTree,
    pipeline_explain: Arc<str>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<S, M, T> fmt::Debug for PhysicalExplain<'_, '_, S, M, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalExplain").finish_non_exhaustive()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalExplain<'env, 'txn, S, M, T>
{
    #[inline]
    pub(crate) fn plan(
        opts: ir::ExplainOptions,
        child: PhysicalNodeId,
        logical_explain: impl Into<Arc<str>>,
        physical_explain: ExplainTree,
        pipeline_explain: impl Into<Arc<str>>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                opts,
                child,
                physical_explain,
                logical_explain: logical_explain.into(),
                pipeline_explain: pipeline_explain.into(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalExplain<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        1
    }

    fn children(&self) -> &[PhysicalNodeId] {
        if self.opts.analyze { std::slice::from_ref(&self.child) } else { &[] }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalExplain<'env, 'txn, S, M, T>
{
    fn initialize(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<()> {
        if self.opts.analyze {
            if self.opts.timing {
                ecx.analyzer().set_mode(AnalyzeMode::Timing);
            } else {
                ecx.analyzer().set_mode(AnalyzeMode::Enabled);
            }
        }

        Ok(())
    }

    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        _tuple: T,
    ) -> ExecutionResult<()> {
        // drop any tuples as we don't really care
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalExplain<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let scx = ecx.scx();

        if self.opts.analyze {
            let metrics = ecx.analyzer().metrics();
            let mut time_annotations =
                ArenaMap::with_capacity(if self.opts.timing { metrics.max_idx() } else { 0 });
            let mut in_tuple_annotations = ArenaMap::with_capacity(metrics.max_idx());
            let mut out_tuple_annotations = ArenaMap::with_capacity(metrics.max_idx());

            for (id, metric) in metrics.iter() {
                if self.opts.timing {
                    time_annotations
                        .insert(id, ("time".to_string(), format!("{:.2?}", metric.elapsed)));
                } else {
                    debug_assert!(
                        metric.elapsed.is_zero(),
                        "shouldn't be calculating timings when not enabled"
                    );
                }

                assert!(
                    in_tuple_annotations
                        .insert(id, ("in".to_string(), metric.tuples_in.to_string()))
                        .is_none()
                );

                assert!(
                    out_tuple_annotations
                        .insert(id, ("out".to_string(), metric.tuples_out.to_string()))
                        .is_none()
                );
            }

            self.physical_explain.annotate(in_tuple_annotations);
            self.physical_explain.annotate(out_tuple_annotations);
            self.physical_explain.annotate(time_annotations);
        }

        let logical_explain = self.logical_explain.to_string();
        let physical_explain = self.physical_explain.to_string();
        let pipeline_explain = self.pipeline_explain.to_string();

        let explain_output = scx.config().explain_output();
        let stringified = match explain_output {
            ExplainOutput::Physical => physical_explain,
            ExplainOutput::Pipeline => pipeline_explain,
            ExplainOutput::Logical => logical_explain,
            ExplainOutput::All => {
                format!(
                    "Logical:\n{logical_explain}\n\nPhysical:\n{physical_explain}\n\nPipeline:\n{pipeline_explain}",
                )
            }
        };

        Ok(Box::new(fallible_iterator::once(T::from_iter([Value::Text(
            stringified.as_str().into(),
        )]))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalExplain<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "explain")?;
        Ok(())
    }
}
