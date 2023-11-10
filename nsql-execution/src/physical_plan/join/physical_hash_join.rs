use nsql_arena::Idx;
use nsql_catalog::expr::Evaluator;

use super::*;
use crate::pipeline::{MetaPipelineBuilder, PipelineBuilder, PipelineBuilderArena};

#[derive(Debug)]
pub(crate) struct PhysicalHashJoin<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 2],
    join_kind: ir::JoinKind,
    conditions: Box<[ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>]>,
    evaluator: Evaluator,
    rhs_width: usize,
    hash_table: HashMap<Tuple, Vec<Tuple>>,
    probe_state: ProbeState,
}

#[derive(Debug, Default)]
enum ProbeState {
    #[default]
    Next,
    Probing {
        rhs_tuples: std::vec::IntoIter<Tuple>,
    },
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashJoin<'env, 'txn, S, M>
{
    pub fn plan(
        join_kind: ir::JoinKind,
        conditions: Box<[ir::JoinCondition<ExecutableExpr<'env, 'txn, S, M>>]>,
        lhs_node: PhysicalNodeId,
        rhs_node: PhysicalNodeId,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalHashJoin<'env, 'txn, S, M>
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalHashJoin<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, ecx))]
    fn execute(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        lhs_tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let storage = ecx.storage();
        let tcx = ecx.tcx();

        let output = loop {
            match &mut self.probe_state {
                ProbeState::Next => {
                    let key = self
                        .conditions
                        .iter()
                        .map(|c| self.evaluator.eval_expr(storage, tcx, &lhs_tuple, &c.lhs))
                        .collect::<Result<Tuple, _>>()?;

                    // FIXME this implements nulls not distinct, we need to implement null distinct too
                    // Could probably have a wrapper type for tuples that implements Eq and Hash but considers nulls not eq
                    match self.hash_table.get(&key) {
                        Some(tuples) => {
                            self.probe_state =
                                ProbeState::Probing { rhs_tuples: tuples.clone().into_iter() }
                        }
                        None => {
                            self.probe_state = ProbeState::Next;
                            break if self.join_kind.is_left() {
                                OperatorState::Yield(lhs_tuple.pad_right(self.rhs_width))
                            } else {
                                OperatorState::Continue
                            };
                        }
                    }
                }
                ProbeState::Probing { rhs_tuples } => match rhs_tuples.next() {
                    Some(rhs_tuple) => {
                        let joint_tuple = lhs_tuple.clone().join(&rhs_tuple);
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalHashJoin<'env, 'txn, S, M>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
        rhs_tuple: Tuple,
    ) -> ExecutionResult<()> {
        tracing::debug!(%rhs_tuple, "building hash join");
        let storage = ecx.storage();
        let tcx = ecx.tcx();

        let key = self
            .conditions
            .iter()
            .inspect(|c| assert_eq!(c.op, ir::JoinOperator::IsNotDistinctFrom, "rest not impl"))
            .map(|c| self.evaluator.eval_expr(storage, tcx, &rhs_tuple, &c.rhs))
            .collect::<Result<Tuple, _>>()?;

        self.hash_table.entry(key).or_default().push(rhs_tuple);

        Ok(())
    }

    fn finalize(&mut self, _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>) -> ExecutionResult<()> {
        self.hash_table.shrink_to_fit();

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalHashJoin<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
        todo!()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalHashJoin<'env, 'txn, S, M>
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
        write!(f, "hash join ({}) ON ...", self.join_kind)?;
        Ok(())
    }
}
