use nsql_storage_engine::StorageEngine;
use parking_lot::RwLock;
use tokio::task::JoinSet;

use super::*;

pub(crate) struct Executor<S> {
    arena: PipelineArena<S>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: StorageEngine> Executor<S> {
    fn execute(
        self: Arc<Self>,
        ctx: ExecutionContext<'_, S>,
        root: Idx<MetaPipeline<S>>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            Arc::clone(&self).execute(ctx.clone(), child);
        }

        for &pipeline in &root.pipelines {
            Arc::clone(&self).execute_pipeline(ctx.clone(), pipeline)?;
            // join_set.spawn(Arc::clone(&self).execute_pipeline(ctx, pipeline));
        }

        Ok(())
    }

    fn execute_pipeline(
        self: Arc<Self>,
        ctx: ExecutionContext<'_, S>,
        pipeline: Idx<Pipeline<S>>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        let mut done = false;
        while !done {
            let chunk = match pipeline.source.source(&ctx)? {
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
                    tuple = match op.execute(&ctx, tuple)? {
                        OperatorState::Yield(tuple) => tuple,
                        OperatorState::Continue => break 'outer,
                        // Once an operator completes, the entire pipeline is finishedK
                        OperatorState::Done => return Ok(()),
                    };
                }

                pipeline.sink.sink(&ctx, tuple)?;
            }
        }

        Ok(())
    }
}

fn execute_root_pipeline<S: StorageEngine>(
    ctx: ExecutionContext<'_, S>,
    pipeline: RootPipeline<S>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(ctx, root)
}

pub fn execute<S: StorageEngine>(
    ctx: ExecutionContext<'_, S>,
    plan: PhysicalPlan<S>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline = build_pipelines(Arc::clone(&sink) as Arc<dyn PhysicalSink<S>>, plan);

    execute_root_pipeline(ctx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference").tuples.into_inner())
}

#[derive(Debug, Default)]
pub(crate) struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl<S: StorageEngine> PhysicalNode<S> for OutputSink {
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &[]
    }

    #[inline]
    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    #[inline]
    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S> for OutputSink {
    fn source(&self, _ctx: &ExecutionContext<'_, S>) -> ExecutionResult<SourceState<Chunk>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSink<S> for OutputSink {
    fn sink(&self, _ctx: &ExecutionContext<'_, S>, tuple: Tuple) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }
}

impl<S: StorageEngine> Explain<S> for OutputSink {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "output")
    }
}
