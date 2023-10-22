use std::marker::PhantomData;
use std::mem;

use nsql_catalog::AggregateFunctionInstance;
use nsql_storage_engine::fallible_iterator;

use super::*;

type AggregateFunctionAndArgs<'env, S, M> = (ir::Function, Option<ExecutableExpr<'env, S, M>>);

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    functions: Box<[AggregateFunctionAndArgs<'env, S, M>]>,
    aggregate_functions: Vec<Box<dyn AggregateFunctionInstance>>,
    children: [PhysicalNodeId; 1],
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        functions: Box<[AggregateFunctionAndArgs<'env, S, M>]>,
        source: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                aggregate_functions: functions
                    .iter()
                    .map(|(f, _args)| f.get_aggregate_instance())
                    .collect(),
                functions,
                children: [source],
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let values = mem::take(&mut self.aggregate_functions).into_iter().map(|f| f.finalize());
        Ok(Box::new(fallible_iterator::once(Tuple::from_iter(values))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        for (state, (_f, expr)) in self.aggregate_functions[..].iter_mut().zip(&self.functions[..])
        {
            let v = expr.as_ref().map(|expr| expr.eval(storage, tx, &tuple)).transpose()?;
            state.update(v);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalUngroupedAggregate<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.functions.len()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    impl_physical_node_conversions!(M; source, sink; not operator);
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
