use super::*;

pub struct PhysicalFilter<'env, 'txn, S, M> {
    children: [Arc<dyn PhysicalNode<'env, 'txn, S, M>>; 1],
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalFilter<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], predicate })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalOperator<'env, 'txn, S, M> for PhysicalFilter<'env, 'txn, S, M>
{
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute(
        &self,
        _ctx: &'txn ExecutionContext<'env, S, M>,
        input: Tuple,
    ) -> ExecutionResult<OperatorState<Tuple>> {
        let value = self.evaluator.evaluate_expr(&input, &self.predicate);
        let keep = value.cast::<bool>(false).expect("this should have failed during planning");
        tracing::debug!(%keep, %input, "filtering tuple");
        // A null predicate is treated as false.
        match keep {
            false => Ok(OperatorState::Continue),
            true => Ok(OperatorState::Yield(input)),
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalFilter<'env, 'txn, S, M>
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &self.children
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S>
    for PhysicalFilter<'env, 'txn, S, M>
{
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "filter {}", self.predicate)?;
        Ok(())
    }
}
