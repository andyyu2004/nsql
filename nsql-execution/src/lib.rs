#![deny(rust_2018_idioms)]
#![feature(trait_upcasting, once_cell_try, anonymous_lifetime_in_impl_trait, exact_size_is_empty)]

mod compile;
pub mod config;
mod executor;
mod physical_plan;
mod pipeline;
mod profiler;

use std::fmt;
use std::ops::{Index, IndexMut};
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;

pub use anyhow::Error;
use dashmap::DashMap;
use executor::OutputSink;
use nsql_arena::{Arena, Idx};
use nsql_catalog::Catalog;
use nsql_core::Name;
use nsql_storage::tuple::Tuple;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
    TransactionConversionHack,
};
use nsql_util::atomic::AtomicEnum;
pub use physical_plan::PhysicalPlanner;
use pipeline::RootPipeline;
use profiler::Profiler;

use self::config::SessionConfig;
pub use self::executor::execute;
use self::physical_plan::{explain, Explain, PhysicalPlan};
use self::pipeline::{
    MetaPipeline, MetaPipelineBuilder, Pipeline, PipelineArena, PipelineBuilder,
    PipelineBuilderArena,
};

pub type ExecutionResult<T, E = Error> = std::result::Result<T, E>;

fn build_pipelines<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    sink: PhysicalNodeId,
    plan: PhysicalPlan<'env, 'txn, S, M>,
) -> RootPipeline<'env, 'txn, S, M> {
    let (mut builder, root_meta_pipeline) =
        PipelineBuilderArena::new(plan.arena()[sink].as_sink().unwrap());
    builder.build(plan.arena(), root_meta_pipeline, plan.root());
    let arena = builder.finish();
    RootPipeline::new(arena, plan.into_arena())
}

struct PhysicalNodeArena<'env, 'txn, S, M> {
    nodes: Arena<Box<dyn PhysicalNode<'env, 'txn, S, M> + 'txn>>,
}

impl<'env, 'txn, S, M> Default for PhysicalNodeArena<'env, 'txn, S, M> {
    fn default() -> Self {
        Self { nodes: Default::default() }
    }
}

impl<'env, 'txn, S, M> Index<PhysicalNodeId> for PhysicalNodeArena<'env, 'txn, S, M> {
    type Output = Box<dyn PhysicalNode<'env, 'txn, S, M> + 'txn>;

    #[inline]
    fn index(&self, index: PhysicalNodeId) -> &Self::Output {
        &self.nodes[index.cast()]
    }
}

impl<'env, 'txn, S, M> IndexMut<PhysicalNodeId> for PhysicalNodeArena<'env, 'txn, S, M> {
    #[inline]
    fn index_mut(&mut self, index: PhysicalNodeId) -> &mut Self::Output {
        &mut self.nodes[index.cast()]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNodeArena<'env, 'txn, S, M>
{
    pub fn alloc_with(
        &mut self,
        mk: impl FnOnce(PhysicalNodeId) -> Box<dyn PhysicalNode<'env, 'txn, S, M> + 'txn>,
    ) -> PhysicalNodeId {
        self.nodes.alloc(mk(self.nodes.next_idx().cast())).cast()
    }
}

type PhysicalNodeId = Idx<()>;

macro_rules! impl_physical_node_conversions {
    ($m_type:ty; $($trait:ident),* $(; not $($not_trait:ident),*)?) => {
        $(
            impl_physical_node_conversions!(@impl $trait, $m_type);
        )*
        $($(
            impl_physical_node_conversions!(@not_impl $not_trait, $m_type);
        )*)?
    };

    (@impl source, $m_type:ty) => {
        fn as_source(
            &self,
        ) -> Result<&dyn PhysicalSource<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
        fn as_source_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalSource<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
    };

    (@impl sink, $m_type:ty) => {
        fn as_sink(
            &self,
        ) -> Result<&dyn PhysicalSink<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
        fn as_sink_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalSink<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
    };

    (@impl operator, $m_type:ty) => {
        fn as_operator(
            &self,
        ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
        fn as_operator_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalOperator<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Ok(self)
        }
    };

    (@not_impl source, $m_type:ty) => {
        fn as_source(
            &self,
        ) -> Result<&dyn PhysicalSource<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
        fn as_source_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalSource<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
    };

    (@not_impl sink, $m_type:ty) => {
        fn as_sink(
            &self,
        ) -> Result<&dyn PhysicalSink<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
        fn as_sink_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalSink<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
    };

    (@not_impl operator, $m_type:ty) => {
        fn as_operator(
            &self,
        ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, $m_type>, &dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
        fn as_operator_mut(
            &mut self,
        ) -> Result<&mut dyn PhysicalOperator<'env, 'txn, S, $m_type>, &mut dyn PhysicalNode<'env, 'txn, S, $m_type>> {
            Err(self)
        }
    };
}

use impl_physical_node_conversions;

// keep this trait crate-private
#[allow(clippy::type_complexity)]
trait PhysicalNode<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    Explain<'env, S> + fmt::Debug
{
    fn id(&self) -> PhysicalNodeId;

    /// The width of the tuples produced by this node
    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize;

    fn children(&self) -> &[PhysicalNodeId];

    fn as_source(
        &self,
    ) -> Result<&dyn PhysicalSource<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>>;

    fn as_source_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalSource<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>;

    fn as_sink(
        &self,
    ) -> Result<&dyn PhysicalSink<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>>;

    fn as_sink_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalSink<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>;

    fn as_operator(
        &self,
    ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>>;

    fn as_operator_mut(
        &mut self,
    ) -> Result<&mut dyn PhysicalOperator<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>;

    // remove this along with the outputsink hack in general
    fn hack_tmp_as_output_sink(&mut self) -> &mut OutputSink<'env, 'txn, S, M> {
        panic!()
    }

    fn build_pipelines(
        &self,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        match self.as_sink() {
            Ok(sink) => {
                // If we have a sink node (which is also a source),
                // - set it to be the source of the current pipeline,
                // - recursively build the pipeline for its child with `sink` as the sink of the new metapipeline
                arena[current].set_source(sink);
                assert!(
                    sink.children().len() <= 1,
                    "default `build_pipelines` implementation only supports unary or nullary nodes for sinks"
                );

                if !sink.children().is_empty() {
                    let child = sink.children()[0];
                    let child_meta_builder = arena.new_child_meta_pipeline(meta_builder, sink);
                    arena.build(nodes, child_meta_builder, child);
                }
            }
            Err(node) => match node.as_source() {
                Ok(source) => {
                    assert!(
                        source.children().is_empty(),
                        "default `build_pipelines` implementation only supports sources at the leaf"
                    );
                    arena[current].set_source(source);
                }
                Err(operator) => {
                    let operator =
                        operator.as_operator().expect("node is not a source, sink, or operator");
                    let children = operator.children();
                    assert_eq!(
                        children.len(),
                        1,
                        "default `build_pipelines` implementation only supports unary operators"
                    );
                    let child = operator.children()[0];
                    arena[current].add_operator(operator);
                    nodes[child].build_pipelines(nodes, arena, meta_builder, current)
                }
            },
        }
    }
}

// generate boilerplate impls of `Explain` and `PhysicalNode` for `&'a mut Trait<'env, 'txn, S, M>`
macro_rules! delegate_physical_node_impl_of_dyn {
    ($ty:ident) => {
        impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
            for &'a mut dyn $ty<'env, 'txn, S, M>
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
                (**self).explain(catalog, tx, f)
            }
        }

        impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
            PhysicalNode<'env, 'txn, S, M> for &'a mut dyn $ty<'env,'txn, S, M>
        {
            fn id(&self) -> PhysicalNodeId {
                (**self).id()
            }

            fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
                (**self).width(nodes)
            }

            fn children(&self) -> &[PhysicalNodeId] {
                (**self).children()
            }

            fn as_source(
                &self,
            ) -> Result<&dyn PhysicalSource<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
                (**self).as_source()
            }

            fn as_source_mut(
                &mut self,
            ) -> Result<&mut dyn PhysicalSource<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
            {
                (**self).as_source_mut()
            }

            fn as_sink(
                &self,
            ) -> Result<&dyn PhysicalSink<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
                (**self).as_sink()
            }

            fn as_sink_mut(
                &mut self,
            ) -> Result<&mut dyn PhysicalSink<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
            {
                (**self).as_sink_mut()
            }

            fn as_operator(
                &self,
            ) -> Result<&dyn PhysicalOperator<'env, 'txn, S, M>, &dyn PhysicalNode<'env, 'txn, S, M>> {
                (**self).as_operator()
            }

            fn as_operator_mut(
                &mut self,
            ) -> Result<&mut dyn PhysicalOperator<'env, 'txn, S, M>, &mut dyn PhysicalNode<'env, 'txn, S, M>>
            {
                (**self).as_operator_mut()
            }
        }

    }
}

delegate_physical_node_impl_of_dyn!(PhysicalNode);
delegate_physical_node_impl_of_dyn!(PhysicalSource);
delegate_physical_node_impl_of_dyn!(PhysicalOperator);
delegate_physical_node_impl_of_dyn!(PhysicalSink);

#[derive(Debug)]
enum OperatorState<T> {
    /// The operator potentially has more output for the same input
    Again(Option<T>),
    /// The operator has an output and is ready to process the next input
    Yield(T),
    /// The operator produced no output for the given input and is ready to process the next input
    Continue,
    /// The operator is done processing input tuples and will never produce more output
    Done,
}

trait PhysicalOperator<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, 'txn, S, M>
{
    fn execute(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>>;
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for &'a mut dyn PhysicalOperator<'env, 'txn, S, M>
{
    fn execute(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        (**self).execute(ecx, input)
    }
}

type TupleStream<'a> = Box<dyn FallibleIterator<Item = Tuple, Error = anyhow::Error> + 'a>;

trait PhysicalSource<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T = Tuple>:
    PhysicalNode<'env, 'txn, S, M>
{
    /// Return the next chunk from the source. An empty chunk indicates that the source is exhausted.
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>>;
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalSource<'env, 'txn, S, M> for &'a mut dyn PhysicalSource<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        (**self).source(ecx)
    }
}

trait PhysicalSink<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>:
    PhysicalSource<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()>;

    fn finalize(&mut self, _ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        Ok(())
    }
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalSource<'env, 'txn, S, M> for &'a mut dyn PhysicalSink<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        (**self).source(ecx)
    }
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalSink<'env, 'txn, S, M> for &'a mut dyn PhysicalSink<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        (**self).sink(ecx, tuple)
    }

    fn finalize(&mut self, ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        (**self).finalize(ecx)
    }
}

pub trait SessionContext {
    fn config(&self) -> &SessionConfig;
}

pub struct TransactionContext<'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    tx: M::Transaction,
    auto_commit: AtomicBool,
    state: AtomicEnum<TransactionState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

impl From<TransactionState> for u8 {
    #[inline]
    fn from(state: TransactionState) -> Self {
        state as u8
    }
}

impl From<u8> for TransactionState {
    #[inline]
    fn from(state: u8) -> Self {
        assert!(state <= TransactionState::Aborted as u8);
        unsafe { std::mem::transmute(state) }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> TransactionContext<'env, S, M> {
    #[inline]
    pub fn new(tx: M::Transaction, auto_commit: bool) -> Self {
        let auto_commit = AtomicBool::new(auto_commit);
        Self { tx, auto_commit, state: AtomicEnum::new(TransactionState::Active) }
    }

    #[inline]
    pub fn auto_commit(&self) -> bool {
        self.auto_commit.load(atomic::Ordering::Acquire)
    }

    #[inline]
    pub fn commit(&self) {
        self.state.store(TransactionState::Committed, atomic::Ordering::Release);
    }

    #[inline]
    pub fn abort(&self) {
        self.state.store(TransactionState::Aborted, atomic::Ordering::Release);
    }

    #[inline]
    pub fn unset_auto_commit(&self) {
        self.auto_commit.store(false, atomic::Ordering::Release)
    }
}

pub struct ExecutionContext<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    catalog: Catalog<'env, S>,
    tcx: TransactionContext<'env, S, M>,
    scx: &'a (dyn SessionContext + 'a),
    materialized_ctes: DashMap<Name, Arc<[Tuple]>>,
    profiler: Profiler,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'_, 'env, S, M> {
    #[inline]
    pub fn take_txn(self) -> (bool, TransactionState, M::Transaction) {
        let tx = self.tcx;
        (tx.auto_commit.into_inner(), tx.state.into_inner(), tx.tx)
    }
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> ExecutionContext<'a, 'env, S, M> {
    #[inline]
    pub fn new(
        catalog: Catalog<'env, S>,
        tcx: TransactionContext<'env, S, M>,
        scx: &'a (dyn SessionContext + 'a),
    ) -> Self {
        Self {
            catalog,
            tcx,
            scx,
            materialized_ctes: Default::default(),
            profiler: Default::default(),
        }
    }

    #[inline]
    pub(crate) fn profiler(&self) -> &Profiler {
        &self.profiler
    }

    #[inline]
    pub fn scx(&self) -> &(dyn SessionContext + '_) {
        self.scx
    }

    #[inline]
    pub fn tcx(&self) -> &TransactionContext<'env, S, M> {
        &self.tcx
    }

    #[inline]
    pub fn tx(&self) -> M::TransactionRef<'_> {
        TransactionConversionHack::as_tx_ref(&self.tcx.tx)
    }

    #[inline]
    pub fn catalog(&self) -> Catalog<'env, S> {
        self.catalog
    }

    pub fn get_materialized_cte_data(&self, name: &Name) -> Arc<[Tuple]> {
        self.materialized_ctes
            .get(name)
            .map(|tuples| Arc::clone(tuples.value()))
            .expect("attempting to get materialized cte data before it is materialized")
    }

    pub fn instantiate_materialized_cte(&self, name: Name, tuples: impl Into<Arc<[Tuple]>>) {
        assert!(
            self.materialized_ctes.insert(name, tuples.into()).is_none(),
            "cte was already materialized"
        );
    }

    #[inline]
    pub fn storage(&self) -> &'env S {
        self.catalog.storage()
    }
}
