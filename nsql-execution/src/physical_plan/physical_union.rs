use std::mem;

use nsql_arena::Idx;
use nsql_storage_engine::fallible_iterator;
use parking_lot::Mutex;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalUnion<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    children: [PhysicalNodeId<'env, 'txn, S, M>; 2],
    buffer: Mutex<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUnion<'env, 'txn, S, M>
{
    pub fn plan(
        lhs: PhysicalNodeId<'env, 'txn, S, M>,
        rhs: PhysicalNodeId<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| {
            Arc::new(Self { id, children: [lhs, rhs], buffer: Default::default() })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        debug_assert_eq!(
            nodes[self.children[0]].width(nodes),
            nodes[self.children[1]].width(nodes)
        );
        nodes[self.children[0]].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn build_pipelines(
        self: Arc<Self>,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        // FIXME it's dumb to need to buffer the full output of both outputs.
        // We could make the union a sort of pseudo operator which does not implement any of the operator traits.
        // Instead it "routes" the output of the children into the same sink.

        // something like the following
        // let sink = arena[meta_builder].sink();
        // let lhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, Arc::clone(&sink));
        // arena.build(lhs_meta_builder, Arc::clone(&self.children[0]));
        //
        // let rhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, Arc::clone(&sink));
        // arena.build(rhs_meta_builder, Arc::clone(&self.children[1].clone()));
        // but it's unclear where to set the source of the current pipeline
        // arena[current].set_source(sink); this would set the the source to itself which is dumb

        let lhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, self.as_ref());
        arena.build(nodes, lhs_meta_builder, self.children[0]);

        let rhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, self.as_ref());
        arena.build(nodes, rhs_meta_builder, self.children[1]);

        arena[current].set_source(self.as_ref());
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let buffer = mem::take(&mut *self.buffer.lock());
        Ok(Box::new(fallible_iterator::convert(buffer.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.buffer.lock().push(tuple);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalUnion<'env, 'txn, S, M>
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
        write!(f, "union")?;
        Ok(())
    }
}
