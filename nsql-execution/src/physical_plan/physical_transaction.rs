use anyhow::bail;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionStmtKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        kind: ir::TransactionStmtKind,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { kind })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalTransaction
{
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalTransaction
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let tx = ctx.tcx();
        match self.kind {
            ir::TransactionStmtKind::Begin(_) => {
                if tx.auto_commit() {
                    tx.unset_auto_commit();
                } else {
                    bail!("nested transactions are not supported")
                }
            }
            ir::TransactionStmtKind::Commit => {
                if tx.auto_commit() {
                    bail!("cannot commit outside of a transaction")
                } else {
                    tx.commit();
                }
            }
            ir::TransactionStmtKind::Abort => {
                if tx.auto_commit() {
                    bail!("cannot rollback outside of a transaction")
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
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        match self.kind {
            ir::TransactionStmtKind::Begin(mode) => write!(f, "begin transaction {}", mode)?,
            ir::TransactionStmtKind::Commit => write!(f, "commit")?,
            ir::TransactionStmtKind::Abort => write!(f, "rollback")?,
        }
        Ok(())
    }
}
