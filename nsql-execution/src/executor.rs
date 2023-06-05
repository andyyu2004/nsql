use nsql_storage_engine::StorageEngine;
use parking_lot::RwLock;

use super::*;

pub(crate) struct Executor<'env, 'txn, S, M> {
    arena: PipelineArena<'env, 'txn, S, M>,
    _marker: std::marker::PhantomData<(S, M)>,
}

impl<'env, 'txn, S: StorageEngine> Executor<'env, 'txn, S, ReadWriteExecutionMode<S>> {
    fn execute(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>,
        root: Idx<MetaPipeline<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            Arc::clone(&self).execute(ctx, child)?;
        }

        for &pipeline in &root.pipelines {
            Arc::clone(&self).execute_pipeline(ctx, pipeline)?;
        }

        Ok(())
    }

    fn execute_pipeline(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>,
        pipeline: Idx<Pipeline<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        let mut stream = Arc::clone(&pipeline.source).source(ctx)?;
        'outer: while let Some(mut tuple) = stream.next()? {
            for op in &pipeline.operators {
                tuple = match op.execute(ctx, tuple)? {
                    OperatorState::Yield(tuple) => tuple,
                    OperatorState::Continue => continue 'outer,
                    // Once an operator completes, the entire pipeline is finished
                    OperatorState::Done => return Ok(()),
                };
            }

            pipeline.sink.sink(ctx, tuple)?;
        }

        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine> Executor<'env, 'txn, S, ReadonlyExecutionMode<S>> {
    fn execute(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, 'txn, S, ReadonlyExecutionMode<S>>,
        root: Idx<MetaPipeline<'env, 'txn, S, ReadonlyExecutionMode<S>>>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            Arc::clone(&self).execute(ctx, child)?;
        }

        for &pipeline in &root.pipelines {
            Arc::clone(&self).execute_pipeline(ctx, pipeline)?;
        }

        Ok(())
    }

    fn execute_pipeline(
        self: Arc<Self>,
        ctx: &ExecutionContext<'env, 'txn, S, ReadonlyExecutionMode<S>>,
        pipeline: Idx<Pipeline<'env, 'txn, S, ReadonlyExecutionMode<S>>>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        let mut stream = Arc::clone(&pipeline.source).source(ctx)?;
        'outer: while let Some(mut tuple) = stream.next()? {
            for op in &pipeline.operators {
                tuple = match op.execute(ctx, tuple)? {
                    OperatorState::Yield(tuple) => tuple,
                    OperatorState::Continue => continue 'outer,
                    // Once an operator completes, the entire pipeline is finished
                    OperatorState::Done => return Ok(()),
                };
            }

            pipeline.sink.sink(ctx, tuple)?;
        }

        Ok(())
    }
}

fn execute_root_pipeline<'env, 'txn, S: StorageEngine>(
    ctx: &ExecutionContext<'env, 'txn, S, ReadonlyExecutionMode<S>>,
    pipeline: RootPipeline<'env, 'txn, S, ReadonlyExecutionMode<S>>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(ctx, root)
}

fn execute_root_pipeline_write<'env, 'txn, S: StorageEngine>(
    ctx: &ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>,
    pipeline: RootPipeline<'env, 'txn, S, ReadWriteExecutionMode<S>>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(ctx, root)
}

pub fn execute<'env, 'txn, S: StorageEngine>(
    ctx: &ExecutionContext<'env, 'txn, S, ReadonlyExecutionMode<S>>,
    plan: PhysicalPlan<'env, 'txn, S, ReadonlyExecutionMode<S>>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline = build_pipelines(
        Arc::clone(&sink) as Arc<dyn PhysicalSink<'env, 'txn, S, ReadonlyExecutionMode<S>>>,
        plan,
    );

    execute_root_pipeline(ctx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference to output sink").tuples.into_inner())
}

pub fn execute_write<'env, 'txn, S: StorageEngine>(
    ctx: &ExecutionContext<'env, 'txn, S, ReadWriteExecutionMode<S>>,
    plan: PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode<S>>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline = build_pipelines(
        Arc::clone(&sink) as Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode<S>>>,
        plan,
    );

    execute_root_pipeline_write(ctx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference to output sink").tuples.into_inner())
}

#[derive(Debug, Default)]
pub(crate) struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for OutputSink
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
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

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for OutputSink
{
    fn source(
        self: Arc<Self>,
        _ctx: &'txn ExecutionContext<'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        todo!()
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for OutputSink
{
    fn sink(
        &self,
        _ctx: &'txn ExecutionContext<'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
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
