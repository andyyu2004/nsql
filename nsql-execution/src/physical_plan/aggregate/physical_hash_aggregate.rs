use std::mem;

use dashmap::DashMap;
use nsql_catalog::{AggregateFunctionInstance, Function};
use nsql_storage_engine::fallible_iterator;
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashAggregate<'env, 'txn, S, M> {
    schema: Schema,
    functions: Box<[(Function, ExecutableExpr)]>,
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    group_expr: ExecutableTupleExpr,
    output_groups: DashMap<Tuple, Mutex<Vec<AggregateFunctionInstance>>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        schema: Schema,
        functions: Box<[(Function, ExecutableExpr)]>,
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        group_expr: ExecutableTupleExpr,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            schema,
            functions,
            group_expr,
            children: [source],
            output_groups: Default::default(),
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let mut output = vec![];
        for entry in self.output_groups.iter() {
            // FIXME is there a way to consume the map without mutable access/ownership? i.e. a drain or something
            let (group, functions) = entry.pair();
            let mut functions = functions.lock();
            let values = mem::take(&mut *functions).into_iter().map(|f| f.finalize());
            output.push(Tuple::from_iter(group.clone().into_iter().chain(values)));
        }

        Ok(Box::new(fallible_iterator::convert(output.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let group = self.group_expr.execute(&tuple);
        let functions = self.output_groups.entry(group).or_insert_with(|| {
            Mutex::new(self.functions.iter().map(|(f, _expr)| f.get_aggregate_instance()).collect())
        });

        let mut aggregate_functions = functions.lock();
        for (state, (_f, expr)) in aggregate_functions.iter_mut().zip(&self.functions[..]) {
            let value = expr.execute(&tuple);
            state.update(value);
        }

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
        fmt: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(
            fmt,
            "hash aggregate ({}) by {}",
            self.functions
                .iter()
                .map(|(f, args)| format!("{}({})", f.name(), args))
                .collect::<Vec<_>>()
                .join(", "),
            self.group_expr
        )?;
        Ok(())
    }
}
