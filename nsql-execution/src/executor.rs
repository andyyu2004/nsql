use std::sync::Arc;

use nsql_arena::Idx;
use nsql_storage::tuple::Tuple;
use parking_lot::RwLock;
use tokio::task::JoinSet;

use crate::physical_plan::PhysicalPlan;
use crate::pipeline::{MetaPipeline, Pipeline, PipelineArena};
use crate::{
    build_pipelines, Chunk, ExecutionContext, ExecutionResult, OperatorState, PhysicalNode,
    PhysicalOperator, PhysicalSink, PhysicalSource, RootPipeline,
};

pub(crate) struct Executor {
    arena: PipelineArena,
}

impl Executor {
    #[async_recursion::async_recursion]
    async fn execute(
        self: Arc<Self>,
        ctx: ExecutionContext,
        root: Idx<MetaPipeline>,
    ) -> ExecutionResult<()> {
        let mut join_set = JoinSet::new();

        let root = &self.arena[root];
        for &child in &root.children {
            join_set.spawn(Arc::clone(&self).execute(ctx.clone(), child));
        }

        while let Some(res) = join_set.join_next().await {
            res??;
        }

        for &pipeline in &root.pipelines {
            join_set.spawn(Arc::clone(&self).execute_pipeline(ctx.clone(), pipeline));
        }

        while let Some(res) = join_set.join_next().await {
            res??;
        }

        assert!(join_set.is_empty());

        Ok(())
    }

    async fn execute_pipeline(
        self: Arc<Self>,
        ctx: ExecutionContext,
        pipeline: Idx<Pipeline>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        loop {
            let chunk = pipeline.source.source(&ctx).await?;
            if chunk.is_empty() {
                break;
            }

            'outer: for mut tuple in chunk {
                for op in &pipeline.operators {
                    tuple = match op.execute(&ctx, tuple).await? {
                        OperatorState::Yield(tuple) => tuple,
                        OperatorState::Continue => break 'outer,
                        // Once an operator completes, the entire pipeline is finishedK
                        OperatorState::Done => return Ok(()),
                    };
                }

                pipeline.sink.sink(&ctx, tuple).await?;
            }
        }

        Ok(())
    }
}

async fn execute_root_pipeline(
    ctx: ExecutionContext,
    pipeline: RootPipeline,
) -> ExecutionResult<()> {
    let executor = Arc::new(Executor { arena: pipeline.arena });
    executor.execute(ctx, pipeline.root).await
}

pub async fn execute(ctx: ExecutionContext, plan: PhysicalPlan) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let root_pipeline = build_pipelines(Arc::clone(&sink) as Arc<dyn PhysicalSink>, plan);

    execute_root_pipeline(ctx, root_pipeline).await?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference").tuples.into_inner())
}

#[derive(Debug, Default)]
struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl PhysicalNode for OutputSink {
    fn desc(&self) -> &'static str {
        "output_sink"
    }

    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for OutputSink {
    fn estimated_cardinality(&self) -> usize {
        self.tuples.read().len()
    }

    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        todo!()
    }
}

#[async_trait::async_trait]
impl PhysicalSink for OutputSink {
    async fn sink(&self, _ctx: &ExecutionContext, tuple: Tuple) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }
}
