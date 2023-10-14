use std::cell::Cell;
use std::fmt;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{ExecutionMode, FallibleIterator, StorageEngine, Transaction};

use crate::physical_plan::{explain, Explain};
use crate::{
    ExecutionContext, ExecutionResult, OperatorState, PhysicalNode, PhysicalNodeArena,
    PhysicalNodeId, PhysicalOperator, PhysicalSink, PhysicalSource, TupleStream,
};

#[derive(Debug)]
pub(crate) struct Profiler {
    timing: Cell<bool>,
    metrics: DashMap<PhysicalNodeId, NodeMetrics>,
}

impl Default for Profiler {
    fn default() -> Self {
        Self { timing: Cell::new(true), metrics: Default::default() }
    }
}

#[derive(Debug, Clone, Copy)]
enum NodeType {
    Misc,
    Source,
    Operator,
    Sink,
}

impl Profiler {
    #[inline]
    pub fn set_timing(&self, enabled: bool) {
        self.timing.set(enabled);
    }

    #[inline]
    fn start(&self, id: PhysicalNodeId, node_type: NodeType) -> ProfilerGuard<'_> {
        let timing = self.timing.get();
        let start = timing.then(Instant::now);
        let tuples_in = matches!(node_type, NodeType::Sink | NodeType::Operator) as usize;
        let tuples_out = matches!(node_type, NodeType::Operator | NodeType::Source) as usize;
        ProfilerGuard { profiler: self, id: id.cast(), start, tuples_in, tuples_out }
    }

    fn init(&self, id: PhysicalNodeId) {
        self.metrics.entry(id).or_default();
    }

    fn record(&self, guard: &ProfilerGuard<'_>) {
        let elapsed = guard.start.map_or(Duration::ZERO, |start| start.elapsed());

        self.metrics
            .entry(guard.id)
            .and_modify(|info| {
                info.elapsed += elapsed;
                info.tuples_in += guard.tuples_in;
                info.tuples_out += guard.tuples_out;
            })
            .or_insert_with(|| NodeMetrics {
                elapsed,
                tuples_in: guard.tuples_in,
                tuples_out: guard.tuples_out,
            });
    }

    pub fn metrics(&self) -> DashMap<PhysicalNodeId, NodeMetrics> {
        self.metrics.clone()
    }
}

#[derive(Debug, Default, Clone)]
pub struct NodeMetrics {
    pub elapsed: Duration,
    /// The number of tuples that entered this node (i.e. operator or sink)
    pub tuples_in: usize,
    /// The number of tuples that this node emitted (i.e. operator or source)
    pub tuples_out: usize,
}

pub(crate) struct ProfilerGuard<'p> {
    id: Idx<()>,
    profiler: &'p Profiler,
    start: Option<Instant>,
    tuples_in: usize,
    tuples_out: usize,
}

impl<'p> Drop for ProfilerGuard<'p> {
    #[inline]
    fn drop(&mut self) {
        self.profiler.record(self);
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
        // ensure the node has an entry in the metrics map even if it never runs
        profiler.init(self.id());
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
    fn id(&self) -> PhysicalNodeId {
        self.node.id()
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.node.width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
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
        let id = self.id();
        let _guard = self.profiler.start(id, NodeType::Misc);
        let iter = self.node.source(ecx)?;
        Ok(Box::new(ProfiledIterator { id, iter, profiler: self.profiler }))
    }
}

struct ProfiledIterator<'p, I> {
    id: Idx<()>,
    profiler: &'p Profiler,
    iter: I,
}

impl<'p, I: FallibleIterator> FallibleIterator for ProfiledIterator<'p, I> {
    type Item = I::Item;

    type Error = I::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        let _guard = self.profiler.start(self.id, NodeType::Source);
        self.iter.next()
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
        let _guard = self.profiler.start(self.id(), NodeType::Operator);
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
        let _guard = self.profiler.start(self.id(), NodeType::Sink);
        self.node.sink(ecx, tuple)
    }

    fn finalize(&mut self, ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        let _guard = self.profiler.start(self.id(), NodeType::Misc);
        self.node.finalize(ecx)
    }
}
