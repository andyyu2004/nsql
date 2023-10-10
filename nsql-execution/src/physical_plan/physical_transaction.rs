use anyhow::bail;
use nsql_storage_engine::fallible_iterator;

use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    kind: ir::TransactionStmt,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalTransaction<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        kind: ir::TransactionStmt,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| Box::new(Self { id, kind }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalTransaction<'env, 'txn, S, M>
{
    impl_physical_node_conversions!(M; source; not operator, sink);

    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        &[]
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalTransaction<'env, 'txn, S, M>
{
    fn source(
        &mut self,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'_>> {
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

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalTransaction<'env, '_, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

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
