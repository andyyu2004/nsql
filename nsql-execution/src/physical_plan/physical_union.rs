use std::marker::PhantomData;
use std::mem;

use nsql_arena::Idx;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalUnion<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    buffer: Vec<Tuple>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUnion<'env, 'txn, S, M>
{
    pub fn plan(
        lhs: PhysicalNodeId,
        rhs: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                children: [lhs, rhs],
                buffer: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        debug_assert_eq!(
            nodes[self.children[0]].width(nodes),
            nodes[self.children[1]].width(nodes)
        );
        nodes[self.children[0]].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    fn build_pipelines(
        &self,
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

        let lhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, self);
        arena.build(nodes, lhs_meta_builder, self.children[0]);

        let rhs_meta_builder = arena.new_child_meta_pipeline(meta_builder, self);
        arena.build(nodes, rhs_meta_builder, self.children[1]);

        arena[current].set_source(self);
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let buffer = mem::take(&mut self.buffer);
        Ok(Box::new(fallible_iterator::convert(buffer.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.buffer.push(tuple);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalUnion<'env, 'txn, S, M>
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
        write!(f, "union")?;
        Ok(())
    }
}
