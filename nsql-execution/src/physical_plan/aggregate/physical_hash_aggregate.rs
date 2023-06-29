use dashmap::DashMap;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashAggregate<'env, 'txn, S, M> {
    schema: Schema,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    group_expr: ExecutableTupleExpr,
    output_groups: DashMap<Tuple, Vec<Tuple>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        schema: Schema,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        group_expr: ExecutableTupleExpr,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { schema, group_expr, children: [source], output_groups: Default::default() })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
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
    for PhysicalHashAggregate<'env, 'txn, S, M>
{
    fn sink(&self, _ecx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        let group = self.group_expr.execute(&tuple);
        self.output_groups.entry(group).or_default().push(tuple);

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
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
    for PhysicalHashAggregate<'env, 'txn, S, M>
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
