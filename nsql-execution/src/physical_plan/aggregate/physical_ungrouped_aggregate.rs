use std::mem;

use nsql_catalog::{AggregateFunction, Function};
use nsql_storage_engine::fallible_iterator;
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate<'env, 'txn, S, M> {
    schema: Schema,
    functions: Box<[(Function, ExecutableExpr)]>,
    aggregate_functions: Mutex<Vec<AggregateFunction>>,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        schema: Schema,
        functions: Box<[(Function, ExecutableExpr)]>,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            schema,
            aggregate_functions: Mutex::new(
                functions.iter().map(|(f, _args)| f.get_aggregate_function()).collect(),
            ),
            functions,
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
        let values = mem::take(&mut *self.aggregate_functions.lock())
            .into_iter()
            .map(|f| f.finalize())
            .collect();
        Ok(Box::new(fallible_iterator::once(Tuple::new(values))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn sink(&self, _ecx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        let mut aggregate_functions = self.aggregate_functions.lock();
        for (state, (_f, expr)) in aggregate_functions.iter_mut().zip(&self.functions[..]) {
            let v = expr.execute(&tuple);
            state.update(v);
        }

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
        fmt: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(
            fmt,
            "ungrouped aggregate ({})",
            self.functions
                .iter()
                .map(|(f, args)| format!("{}({})", f.name(), args))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        Ok(())
    }
}
