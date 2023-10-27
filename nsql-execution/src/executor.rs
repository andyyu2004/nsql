use std::marker::PhantomData;

use nsql_catalog::TransactionContext;

use super::*;
use crate::pipeline::RootPipeline;
use crate::profiler::PhysicalNodeProfileExt;

pub(crate) struct Executor<'env, 'txn, S, M> {
    nodes: PhysicalNodeArena<'env, 'txn, S, M>,
    pipelines: PipelineArena<'env, 'txn, S, M>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Executor<'env, 'txn, S, M> {
    pub(crate) fn new(pipeline: RootPipeline<'env, 'txn, S, M>) -> Self {
        Self { nodes: pipeline.nodes, pipelines: pipeline.arena }
    }

    pub(crate) fn into_pipeline(self) -> RootPipeline<'env, 'txn, S, M> {
        RootPipeline { nodes: self.nodes, arena: self.pipelines }
    }

    fn execute_metapipeline(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        meta_pipeline: Idx<MetaPipeline<'env, 'txn, S, M>>,
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
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        pipeline: Idx<Pipeline<'env, 'txn, S, M>>,
    ) -> ExecutionResult<()> {
        // Safety: caller must ensure the indexes are unique
        unsafe fn get_mut_refs_unchecked<'a, 'env, 'txn, S, M>(
            data: &'a mut PhysicalNodeArena<'env, 'txn, S, M>,
            indices: impl IntoIterator<Item = PhysicalNodeId>,
        ) -> Vec<&'a mut dyn PhysicalNode<'env, 'txn, S, M>> {
            let mut refs = vec![];

            for index in indices {
                let r: *mut dyn PhysicalNode<'env, 'txn, S, M> = data[index].as_mut();
                refs.push(unsafe { &mut *r });
            }

            refs
        }

        let profiler = ecx.profiler();
        let pipeline: &Pipeline<'env, 'txn, S, M> = &self.pipelines[pipeline];
        let node_ids = pipeline.nodes();
        // Safety: a pipeline should never have duplicate nodes
        let mut nodes_mut = unsafe { get_mut_refs_unchecked(&mut self.nodes, node_ids) };
        let [source, operators @ .., sink] = &mut nodes_mut[..] else { panic!() };
        let mut source = (*source).as_source_mut().expect("expected source").profiled(profiler);
        let mut operators = operators
            .iter_mut()
            .map(|op| op.as_operator_mut().expect("expected operator").profiled(profiler))
            .collect::<Box<_>>();
        let mut sink = sink.as_sink_mut().expect("expected sink").profiled(profiler);

        let mut stream = source.source(ecx)?;

        'main_loop: while let Some(tuple) = stream.next()? {
            let mut incomplete_operator_indexes = vec![(0, tuple)];

            'operator_loop: while let Some((operator_idx, mut tuple)) =
                incomplete_operator_indexes.pop()
            {
                tracing::debug!(%tuple, start = %operator_idx, "pushing tuple through pipeline");

                for (idx, op) in operators.iter_mut().enumerate().skip(operator_idx) {
                    let span = tracing::debug_span!(
                        "operator",
                        id= %op.id().into_raw(),
                        "{:#}",
                        op.display(ecx.catalog(), ecx.tcx())
                    );

                    let _entered = span.enter();
                    let input_tuple = tuple;
                    // FIXME avoid clones
                    tuple = match op.execute(ecx, input_tuple.clone())? {
                        OperatorState::Again(tuple) => {
                            incomplete_operator_indexes.push((idx, input_tuple));
                            match tuple {
                                Some(tuple) => {
                                    tracing::trace!(%tuple, "operator state again");
                                    tuple
                                }
                                None => {
                                    tracing::trace!(
                                        "operator state again with no tuple, continuing"
                                    );
                                    continue 'operator_loop;
                                }
                            }
                        }
                        OperatorState::Yield(tuple) => {
                            tracing::debug!(%tuple, "operator state yield");
                            tuple
                        }
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
                sink.sink(ecx, tuple)?;
            }
        }

        sink.finalize(ecx)?;

        Ok(())
    }
}

fn execute_root_pipeline<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    pipeline: RootPipeline<'env, 'txn, S, M>,
) -> ExecutionResult<RootPipeline<'env, 'txn, S, M>> {
    let root = pipeline.arena.root();
    let mut executor = Executor::new(pipeline);
    executor.execute_metapipeline(ecx, root)?;
    Ok(executor.into_pipeline())
}

pub fn execute<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    mut plan: PhysicalPlan<'env, 'txn, S, M>,
) -> ExecutionResult<Vec<Tuple>> {
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
pub(crate) struct OutputSink<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    tuples: Vec<Tuple>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> OutputSink<'env, 'txn, S, M> {
    pub(crate) fn plan(arena: &mut PhysicalNodeArena<'env, 'txn, S, M>) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self { id, tuples: Default::default(), _marker: PhantomData })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    #[inline]
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        unimplemented!(
            "does anyone need to know the width of this one as it will always be at the root?"
        )
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }

    impl_physical_node_conversions!(M; source, sink; not operator);

    fn hack_tmp_as_output_sink(&mut self) -> &mut OutputSink<'env, 'txn, S, M> {
        self
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        unimplemented!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.push(tuple);
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "output")?;
        Ok(())
    }
}
