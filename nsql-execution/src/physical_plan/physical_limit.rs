use std::marker::PhantomData;

use anyhow::bail;

use super::*;

#[derive(Debug)]
pub struct PhysicalLimit<'env, 'txn, S, M> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    yielded: u64,
    limit: u64,
    exceeded_message: Option<String>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalLimit<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        limit: u64,
        exceeded_message: Option<String>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                child: source,
                limit,
                yielded: 0,
                exceeded_message,
                _marker: PhantomData,
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
        self.yielded += 1;

        if self.yielded > self.limit {
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

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
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
