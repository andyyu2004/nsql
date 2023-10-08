use parking_lot::RwLock;

use super::*;
use crate::pipeline::RootPipeline;

pub(crate) struct Executor<'env, 'txn, S, M> {
    nodes: PhysicalNodeArena<'env, 'txn, S, M>,
    pipelines: PipelineArena<'env, 'txn, S, M>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Executor<'env, 'txn, S, M> {
    pub(crate) fn new(pipeline: RootPipeline<'env, 'txn, S, M>) -> Self {
        Self { nodes: pipeline.nodes, pipelines: pipeline.arena }
    }

    fn execute_metapipeline(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        root: Idx<MetaPipeline<'env, 'txn, S, M>>,
    ) -> ExecutionResult<()> {
        let root = &self.pipelines[root];
        for &child in &root.children {
            self.execute_metapipeline(ecx, child)?;
        }

        for &pipeline in &root.pipelines {
            self.execute_pipeline(ecx, pipeline)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, ecx), level = "info")]
    fn execute_pipeline(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        pipeline: Idx<Pipeline<'env, 'txn, S, M>>,
    ) -> ExecutionResult<()> {
        let pipeline: &Pipeline<'env, 'txn, S, M> = &self.pipelines[pipeline];
        let source = self.nodes[pipeline.source].clone().as_source().expect("expected source");
        let operators = &pipeline
            .operators
            .iter()
            .map(|&op| self.nodes[op].clone().as_operator().expect("expected operator"))
            .collect::<Box<_>>();
        let sink = self.nodes[pipeline.sink].clone().as_sink().expect("expected sink");

        let mut stream = source.source(ecx)?;

        'main_loop: while let Some(tuple) = stream.next()? {
            let mut incomplete_operator_indexes = vec![(0, tuple)];

            'operator_loop: while let Some((operator_idx, mut tuple)) =
                incomplete_operator_indexes.pop()
            {
                tracing::debug!(%tuple, start = %operator_idx, "pushing tuple through pipeline");

                for (idx, op) in operators.iter().enumerate().skip(operator_idx) {
                    let span = tracing::debug_span!(
                        "operator",
                        "{:#}",
                        op.display(ecx.catalog(), &ecx.tx())
                    );

                    let _entered = span.enter();
                    let input_tuple = tuple;
                    // FIXME avoid clones
                    tuple = match op.execute(ecx, input_tuple.clone())? {
                        OperatorState::Again(tuple) => {
                            incomplete_operator_indexes.push((idx, input_tuple));
                            match tuple {
                                Some(tuple) => {
                                    tracing::debug!(%tuple, "operator state again");
                                    tuple
                                }
                                None => {
                                    tracing::debug!(
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
                            break 'operator_loop;
                        }
                        // Once an operator completes, the entire pipeline is finished
                        OperatorState::Done => {
                            tracing::debug!("operator state done");
                            break 'main_loop;
                        }
                    };
                }

                let _entered =
                    tracing::debug_span!("sink", "{:#}", sink.display(ecx.catalog(), &ecx.tx()))
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
    ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    pipeline: RootPipeline<'env, 'txn, S, M>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Executor::new(pipeline);
    executor.execute_metapipeline(ecx, root)
}

pub fn execute<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    mut plan: PhysicalPlan<'env, 'txn, S, M>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = OutputSink::plan(plan.arena_mut());
    let root_pipeline = build_pipelines(sink.as_ref(), plan);
    execute_root_pipeline(ecx, root_pipeline)?;
    Ok(Arc::try_unwrap(sink).expect("should be last reference to output sink").tuples.into_inner())
}

// FIXME this is a hack, we shouldn't need this random sink at the root
// We should be able to pull from the executor
#[derive(Debug)]
pub(crate) struct OutputSink<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> OutputSink<'env, 'txn, S, M> {
    pub(crate) fn plan(
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> Arc<OutputSink<'env, 'txn, S, M>> {
        let mut sink = None;
        arena.alloc_with(|id| {
            sink = Some(Arc::new(Self { id, tuples: Default::default() }));
            sink.clone().unwrap()
        });

        sink.unwrap()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    #[inline]
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        unimplemented!(
            "does anyone need to know the width of this one as it will always be at the root?"
        )
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        unimplemented!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for OutputSink<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for OutputSink<'env, '_, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "output")?;
        Ok(())
    }
}
