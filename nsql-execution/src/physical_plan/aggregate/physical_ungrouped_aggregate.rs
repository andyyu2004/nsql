use std::mem;

use nsql_catalog::AggregateFunctionInstance;
use nsql_storage_engine::fallible_iterator;
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate<'env, 'txn, S, M> {
    functions: Box<[(ir::Function, Option<ExecutableExpr<S>>)]>,
    aggregate_functions: Mutex<Vec<Box<dyn AggregateFunctionInstance>>>,
    children: [PhysicalNodeId<'env, 'txn, S, M>; 1],
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        functions: Box<[(ir::Function, Option<ExecutableExpr<S>>)]>,
        source: PhysicalNodeId<'env, 'txn, S, M>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            aggregate_functions: Mutex::new(
                functions.iter().map(|(f, _args)| f.get_aggregate_instance()).collect(),
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
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let values =
            mem::take(&mut *self.aggregate_functions.lock()).into_iter().map(|f| f.finalize());
        Ok(Box::new(fallible_iterator::once(Tuple::from_iter(values))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn sink(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        let mut aggregate_functions = self.aggregate_functions.lock();
        for (state, (_f, expr)) in aggregate_functions.iter_mut().zip(&self.functions[..]) {
            let v = expr.as_ref().map(|expr| expr.execute(storage, &tx, &tuple)).transpose()?;
            state.update(v);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.functions.len()
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
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

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
                .map(|(f, arg)| match arg {
                    Some(arg) => format!("{}({})", f.name(), arg),
                    None => format!("{}(*)", f.name()),
                })
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        Ok(())
    }
}
