use std::mem;

use nsql_core::Name;
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
pub struct PhysicalCte<'env, 'txn, S, M> {
    name: Name,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 2],
    materialized_data: Mutex<Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalCte<'env, 'txn, S, M> {
    pub(crate) fn plan(
        name: Name,
        cte: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        child: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { name, children: [cte, child], materialized_data: Default::default() })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalCte<'env, 'txn, S, M>
{
    fn width(&self) -> usize {
        self.children[1].width()
    }

    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Arc::clone(&self.children[1]).as_source()
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Arc::clone(&self.children[1]).as_operator()
    }

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        let cte_sink = Arc::clone(&self) as Arc<dyn PhysicalSink<'env, 'txn, S, M>>;
        // push data from the cte plan into ourselves
        let cte_builder = arena.new_child_meta_pipeline(meta_builder, cte_sink);
        arena.build(cte_builder, Arc::clone(&self.children[0]));

        // recursively build onto `current` with the child plan
        Arc::clone(&self.children[1]).build_pipelines(arena, meta_builder, current);
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalCte<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        // collect the data from the cte plan
        let mut data = self.materialized_data.lock();
        data.push(tuple);
        Ok(())
    }

    fn finalize(&self, ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        // when finished execution the materialized cte, store the tuples in context for cte scan nodes to consume
        let tuples = mem::take(&mut *self.materialized_data.lock());
        ecx.instantiate_materialized_cte(Name::clone(&self.name), tuples);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalCte<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        unreachable!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalCte<'env, 'txn, S, M>
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
        write!(f, "cte {}", self.name)?;
        Ok(())
    }
}
