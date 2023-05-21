use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan<S: StorageEngine, M: ExecutionMode<S>>(
        kind: ir::TransactionKind,
    ) -> Arc<dyn PhysicalNode<S, M>> {
        Arc::new(Self { kind })
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalNode<S, M> for PhysicalTransaction {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S, M>>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S, M>>, Arc<dyn PhysicalNode<S, M>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine, M: ExecutionMode<S>> PhysicalSource<S, M> for PhysicalTransaction {
    fn source(&self, ctx: &ExecutionContext<'_, '_, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        let tx = ctx.tx();
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

impl<S: StorageEngine> Explain<S> for PhysicalTransaction {
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
