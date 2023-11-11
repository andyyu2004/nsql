use std::marker::PhantomData;
use std::mem;

use nsql_arena::Idx;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalUnion<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    buffer: Vec<T>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalUnion<'env, 'txn, S, M, T>
{
    pub fn plan(
        lhs: PhysicalNodeId,
        rhs: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalUnion<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
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
        nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M, T>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M, T>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M, T>>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalUnion<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let buffer = mem::take(&mut self.buffer);
        Ok(Box::new(fallible_iterator::convert(buffer.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalUnion<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        self.buffer.push(tuple);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalUnion<'env, 'txn, S, M, T>
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
