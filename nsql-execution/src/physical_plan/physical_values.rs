use std::sync::atomic::{self, AtomicUsize};

use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalValues {
    values: ir::Values,
}

impl PhysicalValues {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        values: ir::Values,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(PhysicalValues { values })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalValues
{
    fn source<'txn>(
        self: Arc<Self>,
        _ctx: M::Ref<'txn, ExecutionContext<'env, S, M>>,
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

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalValues
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
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
        Err(self)
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalValues {
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
