use std::fmt;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{ExecutionMode, StorageEngine, Transaction};

use crate::physical_plan::{explain, Explain};
use crate::{
    ExecutionContext, ExecutionResult, OperatorState, PhysicalNode, PhysicalNodeArena,
    PhysicalNodeId, PhysicalOperator, PhysicalSink, PhysicalSource, TupleStream,
};

#[derive(Debug, Default)]
pub(crate) struct Profiler {
    timings: DashMap<Idx<()>, OperatorInfo>,
}

impl Profiler {
    #[inline]
    pub fn start<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        node: &(impl PhysicalNode<'env, 'txn, S, M> + ?Sized),
    ) -> ProfilerSpan<'_> {
        ProfilerSpan { profiler: self, id: node.id().cast(), start: Instant::now() }
    }

    fn record(&self, id: Idx<()>, elapsed: Duration) {
        self.timings
            .entry(id)
            .and_modify(|info| {
                info.elapsed += elapsed;
                info.tuples += 1;
            })
            .or_insert_with(|| OperatorInfo { elapsed, tuples: 1 });
    }
}

#[derive(Debug)]
struct OperatorInfo {
    elapsed: Duration,
    tuples: usize,
}

pub(crate) struct ProfilerSpan<'p> {
    profiler: &'p Profiler,
    id: Idx<()>,
    start: Instant,
}

impl<'p> Drop for ProfilerSpan<'p> {
    #[inline]
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.profiler.record(self.id, elapsed);
    }
}

pub(crate) trait PhysicalNodeProfileExt<'env, 'txn, S, M>: Sized {
    fn profiled(self, profiler: &Profiler) -> ProfiledPhysicalNode<'_, Self>;
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N>
    PhysicalNodeProfileExt<'env, 'txn, S, M> for N
where
    N: PhysicalNode<'env, 'txn, S, M>,
{
    fn profiled(self, profiler: &Profiler) -> ProfiledPhysicalNode<'_, Self> {
        ProfiledPhysicalNode { profiler, node: self }
    }
}

pub(crate) struct ProfiledPhysicalNode<'p, N> {
    profiler: &'p Profiler,
    node: N,
}

impl<'p, N> fmt::Debug for ProfiledPhysicalNode<'p, N>
where
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.node.fmt(f)
    }
}

impl<'p, 'env, S: StorageEngine, N> Explain<'env, S> for ProfiledPhysicalNode<'p, N>
where
    N: Explain<'env, S>,
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        self.node.explain(catalog, tx, f)
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N>
    PhysicalNode<'env, 'txn, S, M> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalNode<'env, 'txn, S, M>,
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.node.id()
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.node.width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        self.node.children()
    }

    fn as_source(
        &self,
    ) -> Result<&dyn PhysicalSource<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
        self.node.as_source()
    }

    fn as_source_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalSource<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
    {
        self.node.as_source_mut()
    }

    fn as_sink(
        &self,
    ) -> Result<&dyn PhysicalSink<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
        self.node.as_sink()
    }

    fn as_sink_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalSink<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
    {
        self.node.as_sink_mut()
    }

    fn as_operator(
        &self,
    ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
        self.node.as_operator()
    }

    fn as_operator_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalOperator<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
    {
        self.node.as_operator_mut()
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N>
    PhysicalSource<'env, 'txn, S, M> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalSource<'env, 'txn, S, M>,
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let _span = self.profiler.start(self);
        let result = self.node.source(ecx)?;
        // TODO need to wrap the iterator in the profiler too so we add to the time on each call the next
        Ok(result)
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N>
    PhysicalOperator<'env, 'txn, S, M> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalOperator<'env, 'txn, S, M>,
{
    fn execute(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let _span = self.profiler.start(self);
        self.node.execute(ecx, input)
    }
}
impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N>
    PhysicalSink<'env, 'txn, S, M> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalSink<'env, 'txn, S, M>,
{
    fn sink(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let _span = self.profiler.start(self);
        self.node.sink(ecx, tuple)
    }

    fn finalize(&mut self, ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        let _span = self.profiler.start(self);
        self.node.finalize(ecx)
    }
}
