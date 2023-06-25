use std::sync::atomic::{self, AtomicBool, AtomicUsize};

use nsql_arena::Idx;
use parking_lot::RwLock;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalNestedLoopJoin<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 2],
    join: ir::Join,
    rhs_tuples: RwLock<Vec<Tuple>>,
    evaluator: Evaluator,
    finalized: AtomicBool,
    rhs_index: AtomicUsize,
    found_match_for_tuple: AtomicBool,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    pub fn plan(
        join: ir::Join,
        probe_node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        build_node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self {
            join,
            children: [probe_node, build_node],
            finalized: AtomicBool::new(false),
            found_match_for_tuple: AtomicBool::new(false),
            rhs_index: AtomicUsize::new(0),
            rhs_tuples: Default::default(),
            evaluator: Evaluator::new(),
        })
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

        Arc::clone(&self.children[0]).build_pipelines(arena, meta_builder, current);

        // create a new meta pipeline for the build side of the join with `self` as the sink
        let child_meta_pipeline =
            arena.new_child_meta_pipeline(meta_builder, Arc::clone(&self) as _);

        arena.build(child_meta_pipeline, Arc::clone(&self.children[1]));
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn execute(
        &self,
        _ecx: &'txn ExecutionContext<'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        assert!(self.finalized.load(atomic::Ordering::Relaxed), "probing before build is finished");

        let rhs_tuples = self.rhs_tuples.read();
        if rhs_tuples.is_empty() {
            return Ok(OperatorState::Done);
        }

        let next_index = match self.rhs_index.fetch_update(
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

                if self.join.is_left()
                    && !self.found_match_for_tuple.swap(false, atomic::Ordering::Relaxed)
                {
                    return Ok(OperatorState::Yield(input.fill_right(3)));
                }
                return Ok(OperatorState::Continue);
            }
        };

        let rhs_tuple = &rhs_tuples[next_index];
        let joint_tuple = input.join(rhs_tuple);

        match &self.join {
            ir::Join::Inner(constraint) | ir::Join::Left(constraint) => match constraint {
                ir::JoinConstraint::On(predicate) => Ok(OperatorState::Again(
                    self.evaluator
                        .evaluate_expr(&joint_tuple, predicate)
                        .cast_non_null::<bool>()?
                        .then(|| {
                            self.found_match_for_tuple.store(true, atomic::Ordering::Relaxed);
                            joint_tuple
                        }),
                )),
            },
            ir::Join::Cross => Ok(OperatorState::Again(Some(joint_tuple))),
            ir::Join::Full(_) => todo!(),
        }
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn sink(&self, _ecx: &'txn ExecutionContext<'env, S, M>, tuple: Tuple) -> ExecutionResult<()> {
        self.rhs_tuples.write().push(tuple);
        Ok(())
    }

    fn finalize(&self, _ecx: &'txn ExecutionContext<'env, S, M>) -> ExecutionResult<()> {
        self.finalized.store(true, atomic::Ordering::Relaxed);
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalNestedLoopJoin<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'env, S, M>,
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
