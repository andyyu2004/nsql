use std::marker::PhantomData;
use std::mem;

use nsql_catalog::AggregateFunctionInstance;
use nsql_storage_engine::fallible_iterator;

use super::*;

type AggregateFunctionAndArgs<'env, 'txn, S, M> =
    (ir::Function, Option<ExecutableExpr<'env, 'txn, S, M>>);

#[derive(Debug)]
pub struct PhysicalUngroupedAggregate<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    functions: Box<[AggregateFunctionAndArgs<'env, 'txn, S, M>]>,
    aggregate_functions: Vec<Box<dyn AggregateFunctionInstance>>,
    children: [PhysicalNodeId; 1],
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalUngroupedAggregate<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        functions: Box<[AggregateFunctionAndArgs<'env, 'txn, S, M>]>,
        source: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
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
                evaluator: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalUngroupedAggregate<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let values = mem::take(&mut self.aggregate_functions).into_iter().map(|f| f.finalize());
        Ok(Box::new(fallible_iterator::once(T::from_iter(values))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalUngroupedAggregate<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        let storage = ecx.storage();
        let tx = ecx.tcx();
        let prof = ecx.profiler();

        for (state, (_f, expr)) in self.aggregate_functions[..].iter_mut().zip(&self.functions[..])
        {
            let v = expr
                .as_ref()
                .map(|expr| expr.eval(&mut self.evaluator, storage, prof, tx, &tuple))
                .transpose()?;
            state.update(v);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalUngroupedAggregate<'env, 'txn, S, M, T>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        self.functions.len()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    impl_physical_node_conversions!(M; source, sink; not operator);
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalUngroupedAggregate<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
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
