use std::sync::atomic::{self, AtomicU64};

use anyhow::bail;

use super::*;

#[derive(Debug)]
pub struct PhysicalLimit<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    child: PhysicalNodeId<'env, 'txn, S, M>,
    yielded: AtomicU64,
    limit: u64,
    exceeded_message: Option<String>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalLimit<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId<'env, 'txn, S, M>,
        limit: u64,
        exceeded_message: Option<String>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                child: source,
                limit,
                yielded: AtomicU64::new(0),
                exceeded_message,
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalLimit<'env, 'txn, S, M>
{
    fn execute(
        &mut self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let yielded = self.yielded.fetch_add(1, atomic::Ordering::AcqRel);
        if yielded >= self.limit {
            if let Some(msg) = &self.exceeded_message {
                bail!("{msg}");
            }

            return Ok(OperatorState::Done);
        }

        Ok(OperatorState::Yield(input))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalLimit<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalLimit<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "limit ({})", self.limit)?;
        Ok(())
    }
}
