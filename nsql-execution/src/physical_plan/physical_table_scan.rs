use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::OnceLock;

use futures_util::{Stream, StreamExt};
use nsql_catalog::{Container, Table};
use tokio::sync::{Mutex, OnceCell};

use super::*;

pub struct PhysicalTableScan {
    table_ref: ir::TableRef,
    table: OnceLock<Arc<Table>>,
    current_batch: Mutex<Vec<Tuple>>,
    current_batch_index: AtomicUsize,

    // mutex here just to make `Self: Sync`
    stream: OnceCell<Mutex<TupleStream>>,
}

type TupleStream = Pin<Box<dyn Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send>>;

impl fmt::Debug for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan").field("table_ref", &self.table_ref).finish()
    }
}

impl PhysicalTableScan {
    pub(crate) fn plan(table_ref: ir::TableRef) -> Arc<dyn PhysicalNode> {
        Arc::new(Self {
            table_ref,
            table: Default::default(),
            current_batch: Default::default(),
            current_batch_index: Default::default(),
            stream: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalTableScan {
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        let tx = ctx.tx();
        let table = self.table.get_or_try_init(|| {
            let namespace = ctx.catalog.get(&tx, self.table_ref.namespace)?.unwrap();
            Ok::<_, nsql_catalog::Error>(namespace.get(&tx, self.table_ref.table)?.unwrap())
        })?;

        // FIXME we can return an entire chunk at a time instead now
        loop {
            let mut next_batch = self.current_batch.lock().await;
            let idx = self.current_batch_index.fetch_add(1, atomic::Ordering::AcqRel);
            if idx < next_batch.len() {
                let tuple = next_batch[idx].clone();
                return Ok(Chunk::singleton(tuple));
            } else {
                let stream = self
                    .stream
                    .get_or_init(|| async move {
                        let storage = table.storage();
                        Mutex::new(Box::pin(storage.scan(ctx.tx()).await) as _)
                    })
                    .await;
                let mut stream = stream.lock().await;
                match stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        *next_batch = batch;
                        self.current_batch_index.store(0, atomic::Ordering::Release);
                    }
                    None => return Ok(Chunk::empty()),
                }
            }
        }
    }

    fn estimated_cardinality(&self) -> usize {
        todo!()
    }
}

impl PhysicalNode for PhysicalTableScan {
    fn desc(&self) -> &'static str {
        "scan"
    }

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
