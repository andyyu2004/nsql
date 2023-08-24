use parking_lot::RwLock;

use super::*;

pub(crate) struct Executor<'env, 'txn, S, M> {
    arena: PipelineArena<'env, 'txn, S, M>,
    _marker: std::marker::PhantomData<(S, M)>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Executor<'env, 'txn, S, M> {
    fn execute(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        root: Idx<MetaPipeline<'env, 'txn, S, M>>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            Arc::clone(&self).execute(ecx, child)?;
        }

        for &pipeline in &root.pipelines {
            Arc::clone(&self).execute_pipeline(ecx, pipeline)?;
        }

        Ok(())
    }

    fn execute_pipeline(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        pipeline: Idx<Pipeline<'env, 'txn, S, M>>,
    ) -> ExecutionResult<()> {
        let pipeline: &Pipeline<'env, 'txn, S, M> = &self.arena[pipeline];
        let mut stream = Arc::clone(&pipeline.source).source(ecx)?;

        'main: while let Some(input_tuple) = stream.next()? {
            'input: loop {
                let mut again = false;
                let mut tuple = input_tuple.clone();

                for op in &pipeline.operators {
                    tuple = match op.execute(ecx, tuple)? {
                        OperatorState::Again(tuple) => match tuple {
                            Some(tuple) => {
                                again = true;
                                tuple
                            }
                            None => continue 'input,
                        },
                        OperatorState::Yield(tuple) => tuple,
                        OperatorState::Continue => continue 'main,
                        // Once an operator completes, the entire pipeline is finished
                        OperatorState::Done => return Ok(()),
                    };
                }

                pipeline.sink.sink(ecx, tuple)?;
                if !again {
                    break 'input;
                }
            }
        }

        pipeline.sink.finalize(ecx)?;

        Ok(())
    }
}

fn execute_root_pipeline<'env, 'txn, S: StorageEngine>(
    ecx: &'txn ExecutionContext<'_, 'env, S, ReadonlyExecutionMode>,
    pipeline: RootPipeline<'env, 'txn, S, ReadonlyExecutionMode>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(ecx, root)
}

fn execute_root_pipeline_write<'env, 'txn, S: StorageEngine>(
    ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    pipeline: RootPipeline<'env, 'txn, S, ReadWriteExecutionMode>,
) -> ExecutionResult<()> {
    let root = pipeline.arena.root();
    let executor = Arc::new(Executor { arena: pipeline.arena, _marker: std::marker::PhantomData });
    executor.execute(ecx, root)
}

pub fn execute<'env: 'txn, 'txn, S: StorageEngine>(
    ecx: &'txn ExecutionContext<'_, 'env, S, ReadonlyExecutionMode>,
    plan: PhysicalPlan<'env, 'txn, S, ReadonlyExecutionMode>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline = build_pipelines(
        Arc::clone(&sink) as Arc<dyn PhysicalSink<'env, 'txn, S, ReadonlyExecutionMode> + 'txn>,
        plan,
    );

    execute_root_pipeline(ecx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference to output sink").tuples.into_inner())
}

pub fn execute_write<'env: 'txn, 'txn, S: StorageEngine>(
    ecx: &'txn ExecutionContext<'_, 'env, S, ReadWriteExecutionMode>,
    plan: PhysicalPlan<'env, 'txn, S, ReadWriteExecutionMode>,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());

    let root_pipeline = build_pipelines(
        Arc::clone(&sink) as Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode> + 'txn>,
        plan,
    );

    execute_root_pipeline_write(ecx, root_pipeline)?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference to output sink").tuples.into_inner())
}

#[derive(Debug, Default)]
pub(crate) struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for OutputSink
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        unimplemented!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for OutputSink
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

impl<S: StorageEngine> Explain<'_, S> for OutputSink {
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
