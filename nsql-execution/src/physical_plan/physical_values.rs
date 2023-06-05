use std::sync::atomic::{self, AtomicUsize};

use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: ir::Values,
}

impl PhysicalValues {
    pub(crate) fn plan<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        values: ir::Values,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(PhysicalValues { values })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalValues
{
    fn source(
        self: Arc<Self>,
        _ctx: &ExecutionContext<'env, 'txn, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let mut index = 0;
        let iter = fallible_iterator::from_fn(move || {
            if index >= self.values.len() {
                return Ok(None);
            }

            let evaluator = Evaluator::new();
            let exprs = &self.values[index];
            let tuple = evaluator.evaluate(&Tuple::empty(), exprs);
            index += 1;

            Ok(Some(tuple))
        });
        Ok(Box::new(iter))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalValues
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
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
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<S> for PhysicalValues {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "scan values")?;
        Ok(())
    }
}
