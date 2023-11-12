use std::marker::PhantomData;
use std::mem;

use nsql_catalog::AggregateFunctionInstance;
use nsql_storage_engine::fallible_iterator;
use rustc_hash::FxHashMap;

use super::*;

type AggregateFunctionAndArgs<'env, 'txn, S, M> =
    (ir::Function, Option<ExecutableExpr<'env, 'txn, S, M>>);

#[derive(Debug)]
pub struct PhysicalHashAggregate<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    aggregates: Box<[AggregateFunctionAndArgs<'env, 'txn, S, M>]>,
    children: [PhysicalNodeId; 1],
    group_expr: ExecutableTupleExpr<'env, 'txn, S, M>,
    output_groups: FxHashMap<T, Vec<Box<dyn AggregateFunctionInstance>>>,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalHashAggregate<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        aggregates: Box<[AggregateFunctionAndArgs<'env, 'txn, S, M>]>,
        source: PhysicalNodeId,
        group_expr: ExecutableTupleExpr<'env, 'txn, S, M>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                aggregates,
                group_expr,
                evaluator: Default::default(),
                children: [source],
                output_groups: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalHashAggregate<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let mut output = vec![];
        for (group, functions) in mem::take(&mut self.output_groups) {
            let values = functions.into_iter().map(|f| f.finalize());
            output.push(T::from_iter(group.clone().into_iter().chain(values)));
        }

        Ok(Box::new(fallible_iterator::convert(output.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalHashAggregate<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        let storage = ecx.storage();
        let tcx = ecx.tcx();
        let prof = ecx.profiler();

        let group = self.group_expr.eval(&mut self.evaluator, storage, prof, tcx, &tuple)?;
        let functions = self.output_groups.entry(group).or_insert_with(|| {
            self.aggregates.iter().map(|(f, _expr)| f.get_aggregate_instance()).collect()
        });

        for (state, (_f, expr)) in functions[..].iter_mut().zip(&self.aggregates[..]) {
            let value = expr
                .as_ref()
                .map(|expr| expr.eval(&mut self.evaluator, storage, prof, tcx, &tuple))
                .transpose()?;
            state.update(value);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalHashAggregate<'env, 'txn, S, M, T>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        self.aggregates.len() + self.group_expr.width()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    impl_physical_node_conversions!(M; source, sink; not operator);
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalHashAggregate<'env, 'txn, S, M, T>
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
            "hash aggregate ({}) by {}",
            self.aggregates
                .iter()
                .map(|(f, arg)| match arg {
                    Some(arg) => format!("{}({})", f.name(), arg),
                    None => format!("{}(*)", f.name()),
                })
                .collect::<Vec<_>>()
                .join(", "),
            self.group_expr
        )?;
        Ok(())
    }
}
