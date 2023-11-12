use nsql_arena::Idx;
use nsql_catalog::expr::Evaluator;
use rustc_hash::FxHashMap;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalHashJoin<'env, 'txn, S, M, T: 'static> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    join_kind: ir::JoinKind,
    conditions: Box<[ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>]>,
    evaluator: Evaluator,
    rhs_width: usize,
    hash_table: FxHashMap<T, Vec<T>>,
    probe_state: ProbeState<T>,
}

#[derive(Debug, Default)]
enum ProbeState<T: 'static> {
    #[default]
    Next,
    Probing {
        // points into the hashtable
        rhs_tuples: std::slice::Iter<'static, T>,
    },
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalHashJoin<'env, 'txn, S, M, T>
{
    pub fn plan(
        join_kind: ir::JoinKind,
        conditions: Box<[ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>]>,
        lhs_node: PhysicalNodeId,
        rhs_node: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        assert!(!join_kind.is_right(), "right hashjoins not implemented");

        let rhs_width = match join_kind {
            ir::JoinKind::Mark => 1,
            _ => arena[rhs_node].width(arena),
        };

        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                join_kind,
                conditions,
                rhs_width,
                children: [lhs_node, rhs_node],
                evaluator: Default::default(),
                hash_table: Default::default(),
                probe_state: Default::default(),
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
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalHashJoin<'env, 'txn, S, M, T>
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
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalHashJoin<'env, 'txn, S, M, T>
{
    #[tracing::instrument(level = "debug", skip(self, ecx))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: &mut T,
    ) -> ExecutionResult<OperatorState<T>> {
        let storage = ecx.storage();
        let tcx = ecx.tcx();

        let output = loop {
            match &mut self.probe_state {
                ProbeState::Next => {
                    let key = self
                        .conditions
                        .iter()
                        .map(|c| self.evaluator.eval_expr(storage, tcx, tuple, &c.lhs))
                        .collect::<Result<T, _>>()?;

                    // FIXME this implements nulls not distinct, we need to implement null distinct too
                    // Could probably have a wrapper type for tuples that implements Eq and Hash but considers nulls not eq
                    match self.hash_table.get(&key) {
                        Some(tuples) => match self.join_kind {
                            ir::JoinKind::Mark => {
                                *tuple = tuple.take().pad_right_with(1, || true);
                                break OperatorState::Yield;
                            }
                            ir::JoinKind::Single => {
                                *tuple = tuple.take().join(&tuples[0]);
                                break OperatorState::Yield;
                            }
                            _ => {
                                self.probe_state = ProbeState::Probing {
                                    // SAFETY: the hashmap is effectively readonly at this point
                                    // and so the value will not move.
                                    rhs_tuples: unsafe { std::mem::transmute(tuples.iter()) },
                                }
                            }
                        },
                        None => {
                            self.probe_state = ProbeState::Next;
                            break if self.join_kind.is_left() {
                                *tuple = match self.join_kind {
                                    ir::JoinKind::Mark => tuple.take().pad_right_with(1, || false),
                                    _ => tuple.take().pad_right(self.rhs_width),
                                };

                                OperatorState::Yield
                            } else {
                                OperatorState::Continue
                            };
                        }
                    }
                }
                ProbeState::Probing { rhs_tuples } => match rhs_tuples.next() {
                    Some(rhs_tuple) => {
                        let joint_tuple = tuple.clone().join(rhs_tuple);
                        break OperatorState::Again(Some(joint_tuple));
                    }
                    None => {
                        self.probe_state = ProbeState::Next;
                        break OperatorState::Continue;
                    }
                },
            }
        };

        Ok(output)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalHashJoin<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        rhs_tuple: T,
    ) -> ExecutionResult<()> {
        tracing::debug!(%rhs_tuple, "building hash join");
        let storage = ecx.storage();
        let tcx = ecx.tcx();

        let key = self
            .conditions
            .iter()
            .inspect(|c| assert_eq!(c.op, ir::JoinOperator::IsNotDistinctFrom, "rest not impl"))
            .map(|c| self.evaluator.eval_expr(storage, tcx, &rhs_tuple, &c.rhs))
            .collect::<Result<T, _>>()?;

        self.hash_table.entry(key).or_default().push(rhs_tuple);

        Ok(())
    }

    fn finalize(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<()> {
        self.hash_table.shrink_to_fit();
        self.hash_table.values_mut().for_each(|v| v.shrink_to_fit());

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalHashJoin<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalHashJoin<'env, 'txn, S, M, T>
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
        write!(f, "hash join ({}) ON ...", self.join_kind)?;
        Ok(())
    }
}
