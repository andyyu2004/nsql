use std::marker::PhantomData;

use nsql_catalog::TransactionContext;

use super::*;
use crate::analyze::PhysicalNodeAnalyzeExt;
use crate::pipeline::RootPipeline;

pub(crate) struct Executor<'env, 'txn, S, M, T> {
    nodes: PhysicalNodeArena<'env, 'txn, S, M, T>,
    pipelines: PipelineArena<'env, 'txn, S, M, T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Executor<'env, 'txn, S, M, T>
{
    pub(crate) fn new(pipeline: RootPipeline<'env, 'txn, S, M, T>) -> Self {
        Self { nodes: pipeline.nodes, pipelines: pipeline.arena }
    }

    pub(crate) fn into_pipeline(self) -> RootPipeline<'env, 'txn, S, M, T> {
        RootPipeline { nodes: self.nodes, arena: self.pipelines }
    }

    fn execute_metapipeline(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        meta_pipeline: Idx<MetaPipeline<'env, 'txn, S, M, T>>,
    ) -> ExecutionResult<()> {
        self.nodes[self.pipelines[meta_pipeline].sink]
            .as_sink_mut()
            .expect("expected sink")
            .initialize(ecx)?;

        let children = self.pipelines[meta_pipeline].children.clone();
        for child in children {
            self.execute_metapipeline(ecx, child)?;
        }

        let pipelines = self.pipelines[meta_pipeline].pipelines.clone();
        for pipeline in pipelines {
            self.execute_pipeline(ecx, pipeline)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, ecx), level = "info")]
    fn execute_pipeline(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        pipeline: Idx<Pipeline<'env, 'txn, S, M, T>>,
    ) -> ExecutionResult<()> {
        let prof = ecx.profiler();
        let _pipeline_guard = prof.start(prof.execute_pipeline);

        // Safety: caller must ensure the indexes are unique
        unsafe fn get_mut_refs_unchecked<'a, 'env, 'txn, S, M, T>(
            data: &'a mut PhysicalNodeArena<'env, 'txn, S, M, T>,
            indices: impl IntoIterator<Item = PhysicalNodeId>,
        ) -> Vec<&'a mut dyn PhysicalNode<'env, 'txn, S, M, T>> {
            let mut refs = vec![];

            for index in indices {
                let r: *mut dyn PhysicalNode<'env, 'txn, S, M, T> = data[index].as_mut();
                refs.push(unsafe { &mut *r });
            }

            refs
        }

        let profiler = ecx.analyzer();
        let pipeline: &Pipeline<'env, 'txn, S, M, T> = &self.pipelines[pipeline];
        let node_ids = pipeline.nodes();
        // Safety: a pipeline should never have duplicate nodes
        let mut nodes_mut = unsafe { get_mut_refs_unchecked(&mut self.nodes, node_ids) };
        let [source, operators @ .., sink] = &mut nodes_mut[..] else { panic!() };
        let mut source = (*source).as_source_mut().expect("expected source").analyze(profiler);
        let mut operators = operators
            .iter_mut()
            .map(|op| op.as_operator_mut().expect("expected operator").analyze(profiler))
            .collect::<Box<_>>();
        let mut sink = sink.as_sink_mut().expect("expected sink").analyze(profiler);

        let mut stream =
            prof.profile(prof.execute_pipeline_create_source, || source.source(ecx))?;

        let mut incomplete_operator_indexes = vec![];

        'main_loop: while let Some(tuple) =
            prof.profile(prof.execute_pipeline_source, || stream.next())?
        {
            let _operators_guard = prof.start(prof.execute_pipeline_execution_loop);

            incomplete_operator_indexes.clear();
            incomplete_operator_indexes.push((0, tuple));

            'operator_loop: while let Some((operator_idx, mut tuple)) =
                incomplete_operator_indexes.pop()
            {
                tracing::debug!(%tuple, start = %operator_idx, "pushing tuple through pipeline");

                for (idx, op) in operators[operator_idx..].iter_mut().enumerate() {
                    let idx = idx + operator_idx;
                    let span = tracing::debug_span!(
                        "operator",
                        id = %op.id().into_raw(),
                        "{:#}",
                        op.display(ecx.catalog(), ecx.tcx())
                    );

                    let _entered = span.enter();
                    match prof
                        .profile(prof.execute_pipeline_operator, || op.execute(ecx, &mut tuple))?
                    {
                        OperatorState::Again(output_tuple) => {
                            // The operator is not allowed to mutate the tuple if it returns `Again` so this is ok assuming the operator is correct.
                            incomplete_operator_indexes.push((idx, tuple));
                            match output_tuple {
                                Some(output_tuple) => {
                                    tuple = output_tuple;
                                    tracing::trace!(%tuple, "operator state again");
                                }
                                None => {
                                    tracing::trace!(
                                        "operator state again with no tuple, continuing"
                                    );
                                    continue 'operator_loop;
                                }
                            }
                        }
                        OperatorState::Yield => tracing::debug!(%tuple, "operator state yield"),
                        OperatorState::Continue => {
                            tracing::debug!("operator state continue");
                            continue 'operator_loop;
                        }
                        // Once an operator completes, the entire pipeline is finished
                        OperatorState::Done => {
                            tracing::debug!("operator state done");
                            break 'main_loop;
                        }
                    };
                }

                let _entered = tracing::debug_span!(
                    "sink",
                    id = %sink.id().into_raw(),
                    "{:#}",
                    sink.display(ecx.catalog(), ecx.tcx())
                )
                .entered();

                tracing::debug!(%tuple, "sinking tuple");

                prof.profile(prof.execute_pipeline_sink, || sink.sink(ecx, tuple))?;
            }
        }

        prof.profile(prof.execute_pipeline_finalize_sink, || sink.finalize(ecx))?;

        Ok(())
    }
}

fn execute_root_pipeline<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>(
    ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    pipeline: RootPipeline<'env, 'txn, S, M, T>,
) -> ExecutionResult<RootPipeline<'env, 'txn, S, M, T>> {
    let root = pipeline.arena.root();
    let mut executor = Executor::new(pipeline);
    executor.execute_metapipeline(ecx, root)?;
    Ok(executor.into_pipeline())
}

pub fn execute<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>(
    ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    mut plan: PhysicalPlan<'env, 'txn, S, M, T>,
) -> ExecutionResult<Vec<T>> {
    let sink = OutputSink::plan(plan.arena_mut());
    let root_pipeline = build_pipelines(sink, plan);
    let mut root_pipeline = execute_root_pipeline(ecx, root_pipeline)?;
    let sink = &mut root_pipeline.nodes[sink];
    let tuples = &mut sink.hack_tmp_as_output_sink().tuples;
    Ok(std::mem::take(tuples))
}

// FIXME this is a hack, we shouldn't need this random sink at the root
// We should be able to pull from the executor
#[derive(Debug)]
pub(crate) struct OutputSink<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    tuples: Vec<T>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    OutputSink<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, tuples: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for OutputSink<'env, 'txn, S, M, T>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    #[inline]
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        unimplemented!(
            "does anyone need to know the width of this one as it will always be at the root?"
        )
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }

    impl_physical_node_conversions!(M; source, sink; not operator);

    fn hack_tmp_as_output_sink(&mut self) -> &mut OutputSink<'env, 'txn, S, M, T> {
        self
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for OutputSink<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        unimplemented!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for OutputSink<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple> Explain<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M, T>
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
        write!(f, "output")?;
        Ok(())
    }
}
