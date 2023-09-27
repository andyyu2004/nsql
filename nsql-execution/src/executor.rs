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

        'main_loop: while let Some(tuple) = stream.next()? {
            let mut incomplete_operator_indexes = vec![(0, tuple)];

            'operator_loop: while let Some((operator_idx, mut tuple)) =
                incomplete_operator_indexes.pop()
            {
                tracing::debug!(%tuple, start = %operator_idx, "pushing tuple through pipeline");

                for (idx, op) in pipeline.operators.iter().enumerate().skip(operator_idx) {
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

                let _entered = tracing::debug_span!(
                    "sink",
                    "{:#}",
                    pipeline.sink.display(ecx.catalog(), &ecx.tx())
                )
                .entered();

                tracing::debug!(%tuple, "sinking tuple");
                pipeline.sink.sink(ecx, tuple)?;
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

impl<'env, S: StorageEngine> Explain<'env, S> for OutputSink {
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
