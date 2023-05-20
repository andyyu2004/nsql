use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan<S: StorageEngine>(kind: ir::TransactionKind) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { kind })
    }
}

impl<S: StorageEngine> PhysicalNode<S> for PhysicalTransaction {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S> for PhysicalTransaction {
    async fn source(&self, ctx: &ExecutionContext<'_, S>) -> ExecutionResult<SourceState<Chunk>> {
        let tx = ctx.tx();
        match self.kind {
            ir::TransactionKind::Begin => {
                if tx.auto_commit() {
                    tx.set_auto_commit(false);
                } else {
                    return Err(nsql_storage::TransactionError::TransactionAlreadyStarted)?;
                }
            }
            ir::TransactionKind::Commit => {
                if tx.auto_commit() {
                    return Err(nsql_storage::TransactionError::CommitWithoutTransaction)?;
                } else {
                    tx.commit().await?;
                }
            }
            ir::TransactionKind::Rollback => {
                if tx.auto_commit() {
                    return Err(nsql_storage::TransactionError::RollbackWithoutTransaction)?;
                } else {
                    tx.rollback().await;
                }
            }
        }

        Ok(SourceState::Done)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalTransaction {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &Transaction,
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
