use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan<'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
        kind: ir::TransactionKind,
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

#[async_trait::async_trait]
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalTransaction
{
    fn source(&self, ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        let _tx = ctx.tx();
        // match self.kind {
        //     ir::TransactionKind::Begin => {
        //         if tx.auto_commit() {
        //             tx.set_auto_commit(false);
        //         } else {
        //             return Err(nsql_storage::TransactionError::TransactionAlreadyStarted)?;
        //         }
        //     }
        //     ir::TransactionKind::Commit => {
        //         if tx.auto_commit() {
        //             return Err(nsql_storage::TransactionError::CommitWithoutTransaction)?;
        //         } else {
        //             tx.commit()?;
        //         }
        //     }
        //     ir::TransactionKind::Rollback => {
        //         if tx.auto_commit() {
        //             return Err(nsql_storage::TransactionError::RollbackWithoutTransaction)?;
        //         } else {
        //             tx.rollback();
        //         }
        //     }
        // }
        todo!();

        Ok(SourceState::Done)
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalTransaction {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        match self.kind {
            ir::TransactionKind::Begin => write!(f, "begin transaction")?,
            ir::TransactionKind::Commit => write!(f, "commit")?,
            ir::TransactionKind::Rollback => write!(f, "rollback")?,
        }
        Ok(())
    }
}
