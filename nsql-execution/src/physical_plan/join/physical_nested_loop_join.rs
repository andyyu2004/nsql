use std::cell::OnceCell;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{self, AtomicBool, AtomicUsize};

use nsql_arena::Idx;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalNestedLoopJoin<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    join_kind: ir::JoinKind,
    join_predicate: ExecutableExpr<'env, S, M>,
    // mutex is only used during build phase
    rhs_tuples_build: Vec<Tuple>,
    // tuples are moved into this vector during finalization (to avoid unnecessary locks)
    rhs_tuples: OnceCell<Vec<Tuple>>,
    rhs_index: AtomicUsize,
    rhs_width: usize,
    found_match_for_lhs_tuple: AtomicBool,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    pub fn plan(
        join_kind: ir::JoinKind,
        join_predicate: ExecutableExpr<'env, S, M>,
        lhs_node: PhysicalNodeId,
        rhs_node: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        assert!(!join_kind.is_right(), "right joins are not supported by nested-loop join");

        let rhs_width = match join_kind {
            ir::JoinKind::Mark => 1,
            _ => arena[rhs_node].width(arena),
        };

        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                join_kind,
                join_predicate,
                rhs_width,
                children: [lhs_node, rhs_node],
                found_match_for_lhs_tuple: AtomicBool::new(false),
                rhs_index: AtomicUsize::new(0),
                rhs_tuples_build: Default::default(),
                rhs_tuples: Default::default(),
                _marker: PhantomData,
            })
        })
    }

    fn lhs_node(&self) -> PhysicalNodeId {
        self.children[0]
    }

    fn rhs_node(&self) -> PhysicalNodeId {
        self.children[1]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source, sink, operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.lhs_node()].width(nodes) + self.rhs_width
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    fn build_pipelines(
        &self,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        // `current` is the probe pipeline of the join
        arena[current].add_operator(self);

        let lhs = self.lhs_node();
        nodes[lhs].build_pipelines(nodes, arena, meta_builder, current);

        // create a new meta pipeline for the build side of the join with `self` as the sink
        let child_meta_pipeline = arena.new_child_meta_pipeline(meta_builder, self);

        arena.build(nodes, child_meta_pipeline, self.rhs_node());
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, ecx))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        lhs_tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let lhs_width = lhs_tuple.width();

        let storage = ecx.storage();
        let tx = ecx.tcx();
        let rhs_tuples = self.rhs_tuples.get().expect("probing before build is finished");

        let rhs_index = match self.rhs_index.fetch_update(
            atomic::Ordering::Relaxed,
            atomic::Ordering::Relaxed,
            |i| {
                if i < rhs_tuples.len() { Some(i + 1) } else { None }
            },
        ) {
            Ok(next_index) => next_index,
            Err(last_index) => {
                assert_eq!(last_index, rhs_tuples.len());
                self.rhs_index.store(0, atomic::Ordering::Relaxed);

                let found_match =
                    self.found_match_for_lhs_tuple.swap(false, atomic::Ordering::Relaxed);
                // emit the lhs_tuple padded with nulls if no match was found
                if !found_match && self.join_kind.is_left() {
                    tracing::trace!(
                        "no match found, emitting tuple padded with nulls for left join"
                    );
                    return match self.join_kind {
                        ir::JoinKind::Mark => {
                            Ok(OperatorState::Yield(lhs_tuple.pad_right_with(1, || false)))
                        }
                        _ => Ok(OperatorState::Yield(lhs_tuple.pad_right(self.rhs_width))),
                    };
                }

                tracing::trace!("completed loop, continuing with next lhs tuple");
                return Ok(OperatorState::Continue);
            }
        };

        let rhs_tuple = &rhs_tuples[rhs_index];
        let joint_tuple = lhs_tuple.join(rhs_tuple);

        let keep = self
            .join_predicate
            .eval(storage, tx, &joint_tuple)?
            .cast::<Option<bool>>()?
            .unwrap_or(false);

        tracing::trace!(%joint_tuple, %keep, "evaluated join predicate");

        if keep {
            tracing::trace!(output = %joint_tuple, "found match, emitting tuple");

            if matches!(self.join_kind, ir::JoinKind::Single) {
                // If this is a single join, we only want to emit one matching tuple.
                // Reset the rhs and continue to the next lhs tuple.
                self.rhs_index.store(0, atomic::Ordering::Relaxed);
                Ok(OperatorState::Yield(joint_tuple))
            } else if matches!(self.join_kind, ir::JoinKind::Mark) {
                // If this is a mark join, we only want to emit the lhs tuple.
                // Reset the rhs and continue to the next lhs tuple.
                self.rhs_index.store(0, atomic::Ordering::Relaxed);
                let lhs_tuple = Tuple::from_iter(
                    joint_tuple.into_iter().take(lhs_width).chain(std::iter::once(true.into())),
                );
                Ok(OperatorState::Yield(lhs_tuple))
            } else {
                self.found_match_for_lhs_tuple.store(true, atomic::Ordering::Relaxed);
                Ok(OperatorState::Again(Some(joint_tuple)))
            }
        } else {
            tracing::debug!("no match found, continuing loop with same lhs tuple");
            Ok(OperatorState::Again(None))
        }
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        tracing::debug!(%tuple, "building nested loop join");
        self.rhs_tuples_build.push(tuple);
        Ok(())
    }

    fn finalize(&mut self, _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>) -> ExecutionResult<()> {
        self.rhs_tuples.set(mem::take(&mut self.rhs_tuples_build)).expect("finalize called twice");
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        if matches!(self.join_kind, ir::JoinKind::Inner) && self.join_predicate.is_literal(true) {
            write!(f, "cross join")?;
        } else {
            write!(f, "nested loop join ({}) ON ({})", self.join_kind, self.join_predicate)?;
        }
        Ok(())
    }
}
