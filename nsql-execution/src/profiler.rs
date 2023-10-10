use std::fmt;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage_engine::{ExecutionMode, StorageEngine, Transaction};

use crate::physical_plan::{explain, Explain};
use crate::{
    ExecutionContext, ExecutionResult, PhysicalNode, PhysicalNodeArena, PhysicalNodeId,
    PhysicalOperator, PhysicalSink, PhysicalSource, TupleStream,
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

    fn record(&self, id: Idx<()>, elapsed: Duration, tuples: usize) {
        self.timings
            .entry(id)
            .and_modify(|info| {
                info.elapsed += elapsed;
                info.tuples += tuples;
            })
            .or_insert_with(|| OperatorInfo { elapsed, tuples });
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

impl<'p> ProfilerSpan<'p> {
    pub fn end(self) -> Duration {
        let elapsed = self.start.elapsed();
        self.profiler.record(self.id, elapsed, 1);
        elapsed
    }
}

impl<'p> Drop for ProfilerSpan<'p> {
    #[inline]
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.profiler.record(self.id, elapsed, 0);
    }
}

pub(crate) trait SourceProfileExt<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    fn profiled<'a>(&'a mut self, profiler: &'a Profiler) -> ProfiledSource<'a, 'env, 'txn, S, M>;
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    SourceProfileExt<'env, 'txn, S, M> for dyn PhysicalSource<'env, 'txn, S, M>
{
    fn profiled<'a>(&'a mut self, profiler: &'a Profiler) -> ProfiledSource<'a, 'env, 'txn, S, M> {
        ProfiledSource { profiler, inner: self }
    }
}

#[derive(Debug)]
pub(crate) struct ProfiledSource<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    profiler: &'a Profiler,
    inner: &'a mut dyn PhysicalSource<'env, 'txn, S, M>,
}

impl<'a, 'env, S: StorageEngine> Explain<'env, S> for &'a mut dyn Explain<'env, S> {
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        (**self).explain(catalog, tx, f)
    }
}
