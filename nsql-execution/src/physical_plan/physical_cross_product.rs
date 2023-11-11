use std::marker::PhantomData;

use nsql_arena::Idx;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalCrossProduct<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    rhs_tuples: Vec<Tuple>,
    rhs_index: usize,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalCrossProduct<'env, 'txn, S, M>
{
    pub fn plan(
        lhs_node: PhysicalNodeId,
        rhs_node: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                children: [lhs_node, rhs_node],
                rhs_index: 0,
                rhs_tuples: Default::default(),
                _marker: PhantomData,
            })
        })
    }

    fn lhs_node(&self) -> PhysicalNodeId {
        self.children[0]
    }

    fn rhs_node(&self) -> PhysicalNodeId {
        self.children[1]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalCrossProduct<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source, sink, operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.lhs_node()].width(nodes) + nodes[self.rhs_node()].width(nodes)
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
        // `current` is the probe pipeline of the join
        arena[current].add_operator(self);

        let lhs = self.lhs_node();
        nodes[lhs].build_pipelines(nodes, arena, meta_builder, current);

        // create a new meta pipeline for the build side of the join with `self` as the sink
        let child_meta_pipeline = arena.new_child_meta_pipeline(meta_builder, self);

        arena.build(nodes, child_meta_pipeline, self.rhs_node());
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalCrossProduct<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, _ecx))]
    fn execute(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        lhs_tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let rhs_tuples = &self.rhs_tuples;

        let rhs_index = match self.rhs_index {
            index if index < rhs_tuples.len() => {
                self.rhs_index += 1;
                index
            }
            last_index => {
                debug_assert_eq!(last_index, rhs_tuples.len());
                self.rhs_index = 0;
                return Ok(OperatorState::Continue);
            }
        };

        let rhs_tuple = &rhs_tuples[rhs_index];
        let joint_tuple = lhs_tuple.join(rhs_tuple);

        Ok(OperatorState::Again(Some(joint_tuple)))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalCrossProduct<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        rhs_tuple: Tuple,
    ) -> ExecutionResult<()> {
        tracing::debug!(%rhs_tuple, "building cross product");
        self.rhs_tuples.push(rhs_tuple);
        Ok(())
    }

    fn finalize(&mut self, _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>) -> ExecutionResult<()> {
        self.rhs_tuples.shrink_to_fit();
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalCrossProduct<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalCrossProduct<'env, 'txn, S, M>
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
        write!(f, "cross product")?;
        Ok(())
    }
}
