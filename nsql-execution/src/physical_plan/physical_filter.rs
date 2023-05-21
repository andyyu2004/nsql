use super::*;

pub struct PhysicalFilter<S, M> {
    children: [Arc<dyn PhysicalNode<S, M>>; 1],
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalFilter<S, M> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<S, M>>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], predicate })
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalOperator<S, M> for PhysicalFilter<S, M> {
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute(
        &self,
        _ctx: &ExecutionContext<'_, '_, S, M>,
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

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalFilter<S, M> {
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &self.children
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> Explain<S> for PhysicalFilter<S, M> {
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
