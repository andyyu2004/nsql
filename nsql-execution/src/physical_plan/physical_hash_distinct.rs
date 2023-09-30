use dashmap::DashSet;

use super::*;

#[derive(Debug)]
pub struct PhysicalHashDistinct<'env, 'txn, S, M> {
    child: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    seen: DashSet<Tuple>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalHashDistinct<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        child: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { child, seen: Default::default() })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalHashDistinct<'env, 'txn, S, M>
{
    #[tracing::instrument(level = "debug", skip(self, _ecx, tuple))]
    fn execute(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        // FIXME can avoid unnecessary tuple clones using `DashMap` raw-api
        let keep = self.seen.insert(tuple.clone());
        tracing::debug!(%keep, %tuple, "deduping tuple");
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(tuple)),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalHashDistinct<'env, 'txn, S, M>
{
    fn width(&self) -> usize {
        self.child.width()
    }

    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        std::slice::from_ref(&self.child)
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalHashDistinct<'env, 'txn, S, M>
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
        write!(f, "hash distinct")?;
        Ok(())
    }
}
