use std::marker::PhantomData;
use std::mem;

use nsql_core::Name;

use super::*;

#[derive(Debug)]
pub struct PhysicalCte<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    name: Name,
    children: [PhysicalNodeId; 2],
    materialized_data: Vec<T>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalCte<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        name: Name,
        cte: PhysicalNodeId,
        child: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                name,
                children: [cte, child],
                materialized_data: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalCte<'env, 'txn, S, M, T>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.children[1]].width(nodes)
    }

    #[inline]
    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    #[inline]
    fn as_sink(
        &self,
    ) -> Result<&dyn PhysicalSink<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        Ok(self)
    }

    #[inline]
    fn as_sink_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalSink<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_source(
        &self,
    ) -> Result<&dyn PhysicalSource<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        Ok(self)
    }

    #[inline]
    fn as_source_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalSource<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        todo!()
        // Arc::clone(&self.children[1]).as_source()
    }

    fn as_operator(
        &self,
    ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        todo!()
    }

    #[inline]
    fn as_operator_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalOperator<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        todo!()
        // Arc::clone(&self.children[1]).as_operator()
    }

    fn build_pipelines(
        &self,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>,
        pipelines: &mut PipelineBuilderArena<'env, 'txn, S, M, T>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M, T>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M, T>>,
    ) {
        // push data from the cte plan into ourselves
        let cte_builder = pipelines.new_child_meta_pipeline(meta_builder, self);
        pipelines.build(nodes, cte_builder, self.children[0]);

        // recursively build onto `current` with the child plan
        let child = self.children[1];
        nodes[child].build_pipelines(nodes, pipelines, meta_builder, current);
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalCte<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        // collect the data from the cte plan
        self.materialized_data.push(tuple);
        Ok(())
    }

    fn finalize(&mut self, ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>) -> ExecutionResult<()> {
        // when finished execution the materialized cte, store the tuples in context for cte scan nodes to consume
        let tuples = mem::take(&mut self.materialized_data);
        ecx.instantiate_materialized_cte(Name::clone(&self.name), tuples);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalCte<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        unreachable!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalCte<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "cte {}", self.name)?;
        Ok(())
    }
}
