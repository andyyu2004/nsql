use std::marker::PhantomData;

use anyhow::bail;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction<'env, 'txn, S, M, T> {
    id: PhysicalNodeId,
    kind: ir::TransactionStmt,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalTransaction<'env, 'txn, S, M, T>
{
    pub(crate) fn plan(
        kind: ir::TransactionStmt,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| Box::new(Self { id, kind, _marker: PhantomData }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalTransaction<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalTransaction<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        let tx = ecx.tcx();
        match self.kind {
            ir::TransactionStmt::Begin(_) => {
                if tx.auto_commit() {
                    tx.no_auto_commit();
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: TupleTrait>
    Explain<'env, 'txn, S, M> for PhysicalTransaction<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
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
