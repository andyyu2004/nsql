use super::*;

#[derive(Debug)]
pub struct PhysicalProjection<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    projection: ExecutableTupleExpr<S>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalProjection<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        projection: ExecutableTupleExpr<S>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { children: [source], projection })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalProjection<'env, 'txn, S, M>
{
    #[tracing::instrument(skip(self, ecx, input))]
    fn execute(
        &self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let storage = ecx.storage();
        let tx = ecx.tx();
        let output = self.projection.execute(storage, &tx, &input)?;
        tracing::debug!(%input, %output, "evaluating projection");
        Ok(OperatorState::Yield(output))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalProjection<'env, 'txn, S, M>
{
    fn width(&self) -> usize {
        self.projection.width()
    }

    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
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
    for PhysicalProjection<'env, 'txn, S, M>
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
        write!(f, "projection (")?;
        fmt::Display::fmt(&self.projection, f)?;
        write!(f, ")")?;
        Ok(())
    }
}
