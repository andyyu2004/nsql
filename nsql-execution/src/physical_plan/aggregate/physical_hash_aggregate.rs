use std::marker::PhantomData;
use std::mem;

use nsql_catalog::AggregateFunctionInstance;
use nsql_storage_engine::fallible_iterator;
use rustc_hash::FxHashMap;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashAggregate<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    aggregates: Box<[(ir::Function, Option<ExecutableExpr<S>>)]>,
    children: [PhysicalNodeId; 1],
    group_expr: ExecutableTupleExpr<S>,
    output_groups: FxHashMap<Tuple, Vec<Box<dyn AggregateFunctionInstance>>>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashAggregate<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        aggregates: Box<[(ir::Function, Option<ExecutableExpr<S>>)]>,
        source: PhysicalNodeId,
        group_expr: ExecutableTupleExpr<S>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                aggregates,
                group_expr,
                children: [source],
                output_groups: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let mut output = vec![];
        for (group, functions) in self.output_groups.iter_mut() {
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
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        let group = self.group_expr.execute(storage, &tx, &tuple)?;
        let functions = self.output_groups.entry(group).or_insert_with(|| {
            self.aggregates.iter().map(|(f, _expr)| f.get_aggregate_instance()).collect()
        });

        for (state, (_f, expr)) in functions[..].iter_mut().zip(&self.aggregates[..]) {
            let value = expr.as_ref().map(|expr| expr.execute(storage, &tx, &tuple)).transpose()?;
            state.update(value);
        }

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalHashAggregate<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.aggregates.len() + self.group_expr.width()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    impl_physical_node_conversions!(M; source, sink; not operator);
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalHashAggregate<'env, 'txn, S, M>
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
