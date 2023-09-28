use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::TupleStream;

#[derive(Debug)]
pub struct PhysicalDummyScan {
    width: Option<usize>,
}

impl PhysicalDummyScan {
    /// If `Some(n)`, then output no tuples (with `width = n`), otherwise output a single empty tuple (width 0).
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        width: Option<usize>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { width })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalDummyScan
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        if self.width.is_some() {
            Ok(Box::new(fallible_iterator::empty()))
        } else {
            Ok(Box::new(fallible_iterator::once(Tuple::empty())))
        }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalDummyScan
{
    fn width(&self) -> usize {
        self.width.unwrap_or(0)
    }

    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
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
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, S> for PhysicalDummyScan {
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        if self.width.is_some() {
            write!(f, "empty")?;
        } else {
            write!(f, "dummy scan")?;
        }
        Ok(())
    }
}
