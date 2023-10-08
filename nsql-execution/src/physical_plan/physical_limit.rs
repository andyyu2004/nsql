use std::sync::atomic::{self, AtomicU64};

use anyhow::bail;

use super::*;

#[derive(Debug)]
pub struct PhysicalLimit<'env, 'txn, S, M> {
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
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { child: source, limit, yielded: AtomicU64::new(0), exceeded_message })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalLimit<'env, 'txn, S, M>
{
    fn execute(
        &self,
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
    fn width(&self, nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        nodes[self.child].width(nodes)
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        std::slice::from_ref(&self.child)
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
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
