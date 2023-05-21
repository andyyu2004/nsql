use nsql_storage_engine::StorageEngine;
use parking_lot::RwLock;

use super::*;

pub(crate) struct Executor<'env, S, M> {
    arena: PipelineArena<'env, S, M>,
    _marker: std::marker::PhantomData<(S, M)>,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Executor<'env, S, M> {
    fn execute(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, S, M>,
        root: Idx<MetaPipeline<'env, S, M>>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            Arc::clone(&self).execute(ctx, child)?;
        }

        for &pipeline in &root.pipelines {
            Arc::clone(&self).execute_pipeline(ctx, pipeline)?;
            // join_set.spawn(Arc::clone(&self).execute_pipeline(ctx, pipeline));
        }

        Ok(())
    }

    fn execute_pipeline(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, S, M>,
        pipeline: Idx<Pipeline<'env, S, M>>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        let mut done = false;
        while !done {
            let chunk = match pipeline.source.source(ctx)? {
                SourceState::Yield(chunk) => chunk,
                SourceState::Final(chunk) => {
                    // run once more around the loop with the final source chunk
                    done = true;
                    chunk
                }
                SourceState::Done => break,
            };

            'outer: for mut tuple in chunk {
                for op in &pipeline.operators {
                    tuple = match op.execute(ctx, tuple)? {
                        OperatorState::Yield(tuple) => tuple,
                        OperatorState::Continue => break 'outer,
                        // Once an operator completes, the entire pipeline is finishedK
                        OperatorState::Done => return Ok(()),
                    };
                }

                pipeline.sink.sink(ctx, tuple)?;
            }
        }

        Ok(())
    }
}

fn execute_root_pipeline<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
    mut ctx: ExecutionContext<'env, S, M>,
    pipeline: RootPipeline<'env, S, M>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(&mut ctx, root)
}

pub fn execute<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
    ctx: ExecutionContext<'env, S, M>,
    plan: PhysicalPlan<'env, S, M>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline =
        build_pipelines(Arc::clone(&sink) as Arc<dyn PhysicalSink<'env, S, M>>, plan);

    execute_root_pipeline(ctx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference").tuples.into_inner())
}

#[derive(Debug, Default)]
pub(crate) struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M> for OutputSink {
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M> for OutputSink {
    fn source(&self, _ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, S, M> for OutputSink {
    fn sink(&self, _ctx: &ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }
}

impl<'env, S: StorageEngine> Explain<S> for OutputSink {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "output")
    }
}
