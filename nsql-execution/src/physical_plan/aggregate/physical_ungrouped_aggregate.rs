use nsql_catalog::{AggregateFunction, Function};

use super::*;

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate<'env, 'txn, S, M> {
    schema: Schema,
    aggregate_function: AggregateFunction,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        schema: Schema,
        function: Function,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        assert_eq!(schema.len(), 1, "ungrouped aggregate can only produce one value");
        let aggregate_function = function.get_aggregate_function();
        Arc::new(Self {
            schema,
            aggregate_function: function.get_aggregate_function(),
            children: [source],
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        todo!();
        // let tuples = mem::take(&mut *self.output.write());
        // Ok(Box::new(fallible_iterator::convert(tuples.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn sink(&self, _ecx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        todo!();
        // self.aggregate_function.update(tuple);
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
    }

    fn schema(&self) -> &[LogicalType] {
        &self.schema
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
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "ungrouped aggregate")?;
        Ok(())
    }
}
