use std::cell::{Cell, Ref, RefCell};
use std::fmt;
use std::time::{Duration, Instant};

use nsql_arena::{ArenaMap, Idx};
use nsql_catalog::{Catalog, TransactionContext};
use nsql_storage::tuple::TupleTrait;
use nsql_storage_engine::{ExecutionMode, FallibleIterator, StorageEngine};

use crate::physical_plan::{explain, Explain};
use crate::{
    ExecutionContext, ExecutionResult, OperatorState, PhysicalNode, PhysicalNodeArena,
    PhysicalNodeId, PhysicalOperator, PhysicalSink, PhysicalSource, TupleStream,
};

#[derive(Debug)]
pub(crate) struct Profiler {
    mode: Cell<ProfileMode>,
    metrics: RefCell<ArenaMap<PhysicalNodeId, NodeMetrics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProfileMode {
    Disabled,
    Enabled,
    Timing,
}

impl Default for Profiler {
    fn default() -> Self {
        Self { mode: Cell::new(ProfileMode::Disabled), metrics: Default::default() }
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
    pub fn set_mode(&self, mode: ProfileMode) {
        self.mode.set(mode);
    }

    fn is_enabled(&self) -> bool {
        // FIXME this is actually pretty slow and can take up a decent amount of runtime even when disabled which sucks
        // Consider using enum dispatch enum { EnabledProfilerImpl | TrivialDisabledProfiledImpl } to avoid having the keep reading the cell.
        self.mode.get() > ProfileMode::Disabled
    }

    #[inline]
    fn start(&self, id: PhysicalNodeId, node_type: NodeType) -> ProfilerGuard<'_> {
        let timing = matches!(self.mode.get(), ProfileMode::Timing);
        let start = timing.then(Instant::now);
        let tuples_in = matches!(node_type, NodeType::Sink | NodeType::Operator) as usize;
        let tuples_out = matches!(node_type, NodeType::Operator | NodeType::Source) as usize;
        ProfilerGuard { profiler: self, id: id.cast(), start, tuples_in, tuples_out }
    }

    fn init(&self, id: PhysicalNodeId) {
        if !self.is_enabled() {
            return;
        }

        self.metrics.borrow_mut().entry(id).or_default();
    }

    fn record(&self, guard: &ProfilerGuard<'_>) {
        if !self.is_enabled() {
            return;
        }

        let elapsed = guard.start.map_or(Duration::ZERO, |start| start.elapsed());
        let mut metrics = self.metrics.borrow_mut();
        let info = metrics.get_mut(guard.id).expect("attempting to record uninitialized node");
        info.elapsed += elapsed;
        info.tuples_in += guard.tuples_in;
        info.tuples_out += guard.tuples_out;
    }

    pub fn metrics(&self) -> Ref<'_, ArenaMap<PhysicalNodeId, NodeMetrics>> {
        self.metrics.borrow()
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

pub(crate) trait PhysicalNodeProfileExt<'env, 'txn, S, M, T>: Sized {
    fn profiled(self, profiler: &Profiler) -> ProfiledPhysicalNode<'_, Self>;
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait, N>
    PhysicalNodeProfileExt<'env, 'txn, S, M, T> for N
where
    N: PhysicalNode<'env, 'txn, S, M, T>,
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

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, N> Explain<'env, 'txn, S, M>
    for ProfiledPhysicalNode<'p, N>
where
    N: Explain<'env, 'txn, S, M>,
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        self.node.explain(catalog, tx, f)
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait, N>
    PhysicalNode<'env, 'txn, S, M, T> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalNode<'env, 'txn, S, M, T>,
{
    fn id(&self) -> PhysicalNodeId {
        self.node.id()
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        self.node.width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        self.node.children()
    }

    fn as_source(
        &self,
    ) -> Result<&dyn PhysicalSource<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        self.node.as_source()
    }

    fn as_source_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalSource<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        self.node.as_source_mut()
    }

    fn as_sink(
        &self,
    ) -> Result<&dyn PhysicalSink<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        self.node.as_sink()
    }

    fn as_sink_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalSink<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        self.node.as_sink_mut()
    }

    fn as_operator(
        &self,
    ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, M, T>, &dyn PhysicalNode<'env, 'txn, S, M, T>>
    {
        self.node.as_operator()
    }

    fn as_operator_mut(
        &mut self,
    ) -> Result<
        &mut dyn PhysicalOperator<'env, 'txn, S, M, T>,
        &mut dyn PhysicalNode<'env, 'txn, S, M, T>,
    > {
        self.node.as_operator_mut()
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait, N>
    PhysicalSource<'env, 'txn, S, M, T> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalSource<'env, 'txn, S, M, T>,
{
    fn source<'s>(
        &'s mut self,
        ecx: &'s ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'s, T>> {
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
        let mut guard = self.profiler.start(self.id, NodeType::Source);
        match self.iter.next()? {
            Some(tuple) => Ok(Some(tuple)),
            None => {
                guard.tuples_out = 0;
                Ok(None)
            }
        }
    }
}

impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait, N>
    PhysicalOperator<'env, 'txn, S, M, T> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalOperator<'env, 'txn, S, M, T>,
{
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>> {
        let mut guard = self.profiler.start(self.id(), NodeType::Operator);
        match self.node.execute(ecx, input)? {
            OperatorState::Again(t) => {
                // don't count the input tuple if it's going to come again
                guard.tuples_in = 0;
                match t {
                    Some(t) => Ok(OperatorState::Again(Some(t))),
                    None => {
                        guard.tuples_out = 0;
                        Ok(OperatorState::Again(None))
                    }
                }
            }
            OperatorState::Yield(t) => Ok(OperatorState::Yield(t)),
            OperatorState::Continue => {
                guard.tuples_out = 0;
                Ok(OperatorState::Continue)
            }
            OperatorState::Done => {
                guard.tuples_out = 0;
                Ok(OperatorState::Done)
            }
        }
    }
}
impl<'p, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait, N>
    PhysicalSink<'env, 'txn, S, M, T> for ProfiledPhysicalNode<'p, N>
where
    N: PhysicalSink<'env, 'txn, S, M, T>,
{
    fn initialize(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<()> {
        let _guard = self.profiler.start(self.id(), NodeType::Misc);
        self.node.initialize(ecx)
    }

    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        let _guard = self.profiler.start(self.id(), NodeType::Sink);
        self.node.sink(ecx, tuple)
    }

    fn finalize(&mut self, ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>) -> ExecutionResult<()> {
        let _guard = self.profiler.start(self.id(), NodeType::Misc);
        self.node.finalize(ecx)
    }
}
