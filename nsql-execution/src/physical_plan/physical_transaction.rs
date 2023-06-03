use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionStmtKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        kind: ir::TransactionStmtKind,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
        Arc::new(Self { kind })
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalTransaction
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, S, M>>, Arc<dyn PhysicalNode<'env, S, M>>> {
        Err(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalTransaction
{
    fn source<'txn>(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let tx = ctx.tx();
        match self.kind {
            ir::TransactionStmtKind::Begin => {
                if tx.auto_commit() {
                    tx.unset_auto_commit();
                } else {
                    todo!()
                }
            }
            ir::TransactionStmtKind::Commit => {
                if tx.auto_commit() {
                    todo!()
                } else {
                    tx.commit();
                }
            }
            ir::TransactionStmtKind::Abort => {
                if tx.auto_commit() {
                    todo!()
                    // return Err(nsql_storage::TransactionError::RollbackWithoutTransaction)?;
                } else {
                    tx.abort();
                }
            }
        }

        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalTransaction {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        match self.kind {
            ir::TransactionStmtKind::Begin => write!(f, "begin transaction")?,
            ir::TransactionStmtKind::Commit => write!(f, "commit")?,
            ir::TransactionStmtKind::Abort => write!(f, "rollback")?,
        }
        Ok(())
    }
}
