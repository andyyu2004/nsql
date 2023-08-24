use std::mem;
use std::sync::atomic::{self, AtomicBool, AtomicUsize};
use std::sync::OnceLock;

use nsql_arena::Idx;
use parking_lot::Mutex;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalNestedLoopJoin<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 2],
    join: ir::Join<ExecutableExpr>,
    // mutex is only used during build phase
    rhs_tuples_build: Mutex<Vec<Tuple>>,
    // tuples are moved into this vector during finalization (to avoid unnecessary locks)
    rhs_tuples: OnceLock<Vec<Tuple>>,
    rhs_index: AtomicUsize,
    found_match_for_tuple: AtomicBool,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    pub fn plan(
        join: ir::Join<ExecutableExpr>,
        probe_node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        build_node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            join,
            children: [probe_node, build_node],
            found_match_for_tuple: AtomicBool::new(false),
            rhs_index: AtomicUsize::new(0),
            rhs_tuples_build: Default::default(),
            rhs_tuples: Default::default(),
        })
    }

    fn probe_node(&self) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::clone(&self.children[0])
    }

    fn build_node(&self) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::clone(&self.children[1])
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn build_pipelines(
        self: Arc<Self>,
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        meta_builder: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        current: Idx<PipelineBuilder<'env, 'txn, S, M>>,
    ) {
        // `current` is the probe pipeline of the join
        arena[current].add_operator(Arc::clone(&self) as _);

        self.probe_node().build_pipelines(arena, meta_builder, current);

        // create a new meta pipeline for the build side of the join with `self` as the sink
        let child_meta_pipeline =
            arena.new_child_meta_pipeline(meta_builder, Arc::clone(&self) as _);

        arena.build(child_meta_pipeline, self.build_node());
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn execute(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        lhs_tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let lhs_width = lhs_tuple.width();
        let rhs_tuples = self.rhs_tuples.get().expect("probing before build is finished");
        if rhs_tuples.is_empty() {
            return Ok(OperatorState::Done);
        }

        let rhs_tuple_width = rhs_tuples[0].width();

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

                let found_match = self.found_match_for_tuple.swap(false, atomic::Ordering::Relaxed);
                // emit the lhs_tuple padded with nulls if no match was found
                if !found_match && self.join.is_left() {
                    return Ok(OperatorState::Yield(lhs_tuple.fill_right(rhs_tuple_width)));
                }
                return Ok(OperatorState::Continue);
            }
        };

        let rhs_tuple = &rhs_tuples[rhs_index];
        let joint_tuple = lhs_tuple.join(rhs_tuple);

        match &self.join {
            ir::Join::Constrained(_kind, constraint) => {
                let keep = match constraint {
                    ir::JoinConstraint::On(predicate) => {
                        predicate.execute(&joint_tuple).cast::<Option<bool>>()?.unwrap_or(false)
                    }
                };

                if keep {
                    self.found_match_for_tuple.store(true, atomic::Ordering::Relaxed);
                    Ok(OperatorState::Again(Some(joint_tuple)))
                } else if rhs_index == rhs_tuples.len() - 1
                    && self.join.is_right()
                    && !self.found_match_for_tuple.load(atomic::Ordering::Relaxed)
                {
                    // emit a rhs_tuple padded with nulls if no match was found
                    Ok(OperatorState::Again(Some(rhs_tuple.clone().fill_left(lhs_width))))
                } else {
                    Ok(OperatorState::Again(None))
                }
            }
            // ir::Join::Inner(constraint)
            // | ir::Join::Left(constraint)
            // | ir::Join::Right(constraint)
            // | ir::Join::Full(constraint) => {
            ir::Join::Cross => Ok(OperatorState::Again(Some(joint_tuple))),
        }
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        self.rhs_tuples_build.lock().push(tuple);
        Ok(())
    }

    fn finalize(&self, _ecx: &'txn ExecutionContext<'_, 'env, S, M>) -> ExecutionResult<()> {
        self.rhs_tuples
            .set(mem::take(&mut self.rhs_tuples_build.lock()))
            .expect("finalize called twice");
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'_, S>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "nested loop join ({})", self.join)?;
        Ok(())
    }
}
