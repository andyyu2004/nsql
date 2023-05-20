use super::*;

pub struct PhysicalFilter<S> {
    children: [Arc<dyn PhysicalNode<S>>; 1],
    predicate: ir::Expr,
    evaluator: Evaluator,
}

impl<S: StorageEngine> PhysicalFilter<S> {
    pub(crate) fn plan(
        source: Arc<dyn PhysicalNode<S>>,
        predicate: ir::Expr,
    ) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { evaluator: Evaluator::new(), children: [source], predicate })
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalOperator<S> for PhysicalFilter<S> {
    #[tracing::instrument(skip(self, _ctx, input))]
    fn execute(
        &self,
        _ctx: &ExecutionContext<'_, S>,
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

impl<S: StorageEngine> PhysicalNode<S> for PhysicalFilter<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &self.children
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalFilter<S> {
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
