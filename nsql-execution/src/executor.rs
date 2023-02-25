use std::sync::Arc;

use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_transaction::Transaction;
use parking_lot::RwLock;

use crate::arena::Idx;
use crate::physical_plan::PhysicalPlan;
use crate::pipeline::{MetaPipeline, Pipeline, PipelineArena};
use crate::{
    build_pipelines, ExecutionContext, ExecutionResult, PhysicalNode, PhysicalOperator,
    PhysicalSink, PhysicalSource,
};

pub(crate) struct Executor {
    arena: PipelineArena,
}

impl Executor {
    #[async_recursion::async_recursion]
    async fn execute(
        &self,
        ctx: &ExecutionContext<'_>,
        root: Idx<MetaPipeline>,
    ) -> ExecutionResult<()> {
        let root = &self.arena[root];
        for &child in &root.children {
            self.execute(ctx, child).await?;
        }

        for &pipeline in &root.pipelines {
            self.execute_pipeline(ctx, pipeline).await?;
        }

        Ok(())
    }

    async fn execute_pipeline(
        &self,
        ctx: &ExecutionContext<'_>,
        pipeline: Idx<Pipeline>,
    ) -> ExecutionResult<()> {
        let pipeline = &self.arena[pipeline];
        while let Some(mut tuple) = pipeline.source.source(ctx).await? {
            for op in &pipeline.operators {
                tuple = op.execute(ctx, tuple).await?;
            }
            pipeline.sink.sink(ctx, tuple).await?;
        }
        Ok(())
    }
}

async fn execute_meta_pipeline(
    tx: &Transaction,
    catalog: &Catalog,
    arena: PipelineArena,
    root: Idx<MetaPipeline>,
) -> ExecutionResult<()> {
    let ctx = ExecutionContext::new(tx, catalog);
    let executor = Executor { arena };
    executor.execute(&ctx, root).await
}

pub async fn execute(
    tx: &Transaction,
    catalog: &Catalog,
    plan: PhysicalPlan,
) -> ExecutionResult<Vec<Tuple>> {
    let sink = Arc::new(OutputSink::default());
    let (arena, root) = build_pipelines(Arc::clone(&sink) as Arc<dyn PhysicalSink>, plan);

    execute_meta_pipeline(tx, catalog, arena, root.cast()).await?;

    Ok(Arc::try_unwrap(sink).expect("should be last reference").tuples.into_inner())
}

#[derive(Debug, Default)]
struct OutputSink {
    tuples: RwLock<Vec<Tuple>>,
}

impl PhysicalNode for OutputSink {
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

    async fn source(&self, _ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl PhysicalSink for OutputSink {
    async fn sink(&self, _ctx: &ExecutionContext<'_>, tuple: Tuple) -> ExecutionResult<()> {
        self.tuples.write().push(tuple);
        Ok(())
    }
}
