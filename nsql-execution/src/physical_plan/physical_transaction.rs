use anyhow::bail;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionStmt,
}

impl PhysicalTransaction {
    pub(crate) fn plan<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        kind: ir::TransactionStmt,
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

    fn schema(&self) -> &[LogicalType] {
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
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tx = ecx.tcx();
        match self.kind {
            ir::TransactionStmt::Begin(_) => {
                if tx.auto_commit() {
                    tx.unset_auto_commit();
                } else {
                    bail!("nested transactions are not supported")
                }
            }
            ir::TransactionStmt::Commit => {
                if tx.auto_commit() {
                    bail!("cannot commit outside of a transaction")
                } else {
                    tx.commit();
                }
            }
            ir::TransactionStmt::Abort => {
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

impl<S: StorageEngine> Explain<'_, S> for PhysicalTransaction {
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        match self.kind {
            ir::TransactionStmt::Begin(mode) => write!(f, "begin transaction {}", mode)?,
            ir::TransactionStmt::Commit => write!(f, "commit")?,
            ir::TransactionStmt::Abort => write!(f, "rollback")?,
        }
        Ok(())
    }
}
