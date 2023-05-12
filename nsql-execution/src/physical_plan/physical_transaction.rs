use super::*;

#[derive(Debug)]
pub struct PhysicalTransaction {
    kind: ir::TransactionKind,
}

impl PhysicalTransaction {
    pub(crate) fn plan(kind: ir::TransactionKind) -> Arc<dyn PhysicalNode> {
        Arc::new(Self { kind })
    }
}

impl PhysicalNode for PhysicalTransaction {
    fn children(&self) -> &[Arc<dyn PhysicalNode>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource>, Arc<dyn PhysicalNode>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink>, Arc<dyn PhysicalNode>> {
        Err(self)
    }

    fn as_operator(self: Arc<Self>) -> Result<Arc<dyn PhysicalOperator>, Arc<dyn PhysicalNode>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalTransaction {
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        let tx = ctx.tx();
        match self.kind {
            ir::TransactionKind::Begin => {
                if tx.auto_commit() {
                    tx.set_auto_commit(false);
                } else {
                    return Err(nsql_transaction::TransactionError::TransactionAlreadyStarted)?;
                }
            }
            ir::TransactionKind::Commit => {
                if tx.auto_commit() {
                    return Err(nsql_transaction::TransactionError::CommitWithoutTransaction)?;
                } else {
                    tx.commit().await?;
                }
            }
            ir::TransactionKind::Rollback => {
                if tx.auto_commit() {
                    return Err(nsql_transaction::TransactionError::RollbackWithoutTransaction)?;
                } else {
                    tx.rollback().await;
                }
            }
        }

        Ok(SourceState::Done)
    }
}

impl Explain for PhysicalTransaction {
    fn explain(
        &self,
        _catalog: &Catalog,
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
