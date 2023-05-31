use super::*;

pub struct PhysicalFilter<'env, S, M> {
    children: [Arc<dyn PhysicalNode<'env, S, M>>; 1],
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalFilter<'env, S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<'env, S, M>>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], predicate })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalOperator<'env, S, M>
    for PhysicalFilter<'env, S, M>
{
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute<'txn>(
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

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalFilter<'env, S, M>
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &self.children
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<S> for PhysicalFilter<'env, S, M> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "filter {}", self.predicate)?;
        Ok(())
    }
}
