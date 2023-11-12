use std::marker::PhantomData;

use nsql_arena::Idx;
use nsql_catalog::expr::Evaluator;
use nsql_storage::tuple::JointTuple;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalNestedLoopJoin<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    join_kind: ir::JoinKind,
    join_predicate: ExecutableExpr<'env, 'txn, S, M>,
    rhs_tuples: Vec<T>,
    rhs_index: usize,
    rhs_width: usize,
    found_match_for_lhs_tuple: bool,
    evaluator: Evaluator,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    pub fn plan(
        join_kind: ir::JoinKind,
        join_predicate: ExecutableExpr<'env, 'txn, S, M>,
        lhs_node: PhysicalNodeId,
        rhs_node: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
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
                found_match_for_lhs_tuple: false,
                rhs_index: 0,
                rhs_tuples: Default::default(),
                evaluator: Default::default(),
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source, sink, operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.lhs_node()].width(nodes) + self.rhs_width
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }

    fn build_pipelines(
        &self,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M, T>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M, T>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M, T>>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    #[tracing::instrument(level = "debug", skip(self, ecx))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: &mut T,
    ) -> ExecutionResult<OperatorState<T>> {
        let lhs_width = tuple.width();

        let storage = ecx.storage();
        let prof = ecx.profiler();
        let tcx = ecx.tcx();
        let rhs_tuples = &self.rhs_tuples;

        let _guard = prof.start(prof.nlp_join_execute);

        let rhs_index = match self.rhs_index {
            index if index < rhs_tuples.len() => {
                self.rhs_index += 1;
                index
            }
            last_index => {
                debug_assert_eq!(last_index, rhs_tuples.len());
                self.rhs_index = 0;

                let found_match = self.found_match_for_lhs_tuple;
                self.found_match_for_lhs_tuple = false;
                // emit the lhs_tuple padded with nulls if no match was found
                if !found_match && self.join_kind.is_left() {
                    tracing::trace!(
                        "no match found, emitting tuple padded with nulls for left join"
                    );
                    *tuple = match self.join_kind {
                        ir::JoinKind::Mark => tuple.take().pad_right_with(1, || false),
                        _ => tuple.take().pad_right(self.rhs_width),
                    };
                    return Ok(OperatorState::Yield);
                }

                tracing::trace!("completed loop, continuing with next lhs tuple");
                return Ok(OperatorState::Continue);
            }
        };

        let rhs_tuple = &rhs_tuples[rhs_index];
        // workaround to evaluate against a joint tuple without allocating and copying
        let tmp_joint_tuple = JointTuple(tuple, rhs_tuple);

        let keep = self
            .join_predicate
            .eval(&mut self.evaluator, storage, prof, tcx, &tmp_joint_tuple)?
            .cast::<Option<bool>>()?
            .unwrap_or(false);

        tracing::trace!(%keep, "evaluated join predicate");

        if keep {
            // only perform the necessary clone if we're keeping the tuple
            let joint_tuple = tuple.clone().join(rhs_tuple);
            tracing::trace!(output = %joint_tuple, "found match, emitting tuple");

            if matches!(self.join_kind, ir::JoinKind::Single) {
                // If this is a single join, we only want to emit one matching tuple.
                // Reset the rhs and continue to the next lhs tuple.
                self.rhs_index = 0;

                *tuple = joint_tuple;
                Ok(OperatorState::Yield)
            } else if matches!(self.join_kind, ir::JoinKind::Mark) {
                // If this is a mark join, we only want to emit the lhs tuple.
                // Reset the rhs and continue to the next lhs tuple.
                self.rhs_index = 0;
                let lhs_tuple = T::from_iter(
                    joint_tuple.into_iter().take(lhs_width).chain(std::iter::once(true.into())),
                );
                *tuple = lhs_tuple;
                Ok(OperatorState::Yield)
            } else {
                self.found_match_for_lhs_tuple = true;
                Ok(OperatorState::Again(Some(joint_tuple)))
            }
        } else {
            tracing::debug!("no match found, continuing loop with same lhs tuple");
            Ok(OperatorState::Again(None))
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        rhs_tuple: T,
    ) -> ExecutionResult<()> {
        tracing::debug!(%rhs_tuple, "building nested loop join");
        self.rhs_tuples.push(rhs_tuple);
        Ok(())
    }

    fn finalize(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<()> {
        self.rhs_tuples.shrink_to_fit();
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalNestedLoopJoin<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "nested loop join ({}) ON ({})", self.join_kind, self.join_predicate)?;
        Ok(())
    }
}
