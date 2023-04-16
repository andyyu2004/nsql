use std::sync::atomic::{self, AtomicUsize};
use std::sync::OnceLock;

use nsql_catalog::{Container, Table};
use parking_lot::Mutex;

use super::*;

#[derive(Debug)]
pub struct PhysicalTableScan {
    table_ref: ir::TableRef,
    table: OnceLock<Arc<Table>>,
    next_batch: Mutex<Vec<Tuple>>,
    batch_index: AtomicUsize,
}

impl PhysicalTableScan {
    pub(crate) fn plan(table_ref: ir::TableRef) -> Arc<dyn PhysicalNode> {
        Arc::new(Self {
            table_ref,
            table: OnceLock::new(),
            next_batch: Default::default(),
            batch_index: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalTableScan {
    async fn source(&self, ctx: &ExecutionContext<'_>) -> ExecutionResult<Option<Tuple>> {
        let table = self.table.get_or_try_init(|| {
            let namespace = ctx.catalog.get(ctx.tx, self.table_ref.namespace)?.unwrap();
            Ok::<_, nsql_catalog::Error>(namespace.get(ctx.tx, self.table_ref.table)?.unwrap())
        })?;

        let storage = table.storage();

        // FIXME tmp hack impl
        let tuples = storage.scan(ctx.tx).await?;
        {
            let mut next_batch = self.next_batch.lock();
            *next_batch = tuples;
            let idx = self.batch_index.load(atomic::Ordering::Acquire);
            if idx < next_batch.len() {
                let tuple = next_batch[idx].clone();
                self.batch_index.fetch_add(1, atomic::Ordering::Release);
                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }

    fn estimated_cardinality(&self) -> usize {
        todo!()
    }
}

impl PhysicalNode for PhysicalTableScan {
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
