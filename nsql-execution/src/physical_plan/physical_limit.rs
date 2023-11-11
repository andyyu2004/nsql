use std::marker::PhantomData;

use anyhow::bail;
use nsql_storage::value::Text;

use super::*;

#[derive(Debug)]
pub struct PhysicalLimit<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    child: PhysicalNodeId,
    yielded: u64,
    limit: u64,
    exceeded_message: Option<Text>,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalLimit<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        source: PhysicalNodeId,
        limit: u64,
        exceeded_message: Option<Text>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalOperator<'env, 'txn, S, M, T> for PhysicalLimit<'env, 'txn, S, M, T>
{
    fn execute(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        input: T,
    ) -> ExecutionResult<OperatorState<T>> {
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalLimit<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; operator; not source, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.child)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    Explain<'env, 'txn, S, M> for PhysicalLimit<'env, 'txn, S, M, T>
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
        write!(f, "limit ({})", self.limit)?;
        Ok(())
    }
}
