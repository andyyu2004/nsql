use std::pin::Pin;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::OnceLock;

use futures_util::{Stream, StreamExt};
use itertools::Itertools;
use nsql_catalog::{Column, ColumnIndex, Container, Entity, Table};
use nsql_storage::tuple::TupleIndex;
use tokio::sync::{Mutex, OnceCell};

use super::*;

pub struct PhysicalTableScan {
    table_ref: ir::TableRef,
    table: OnceLock<Arc<Table>>,
    projection: Option<Box<[ColumnIndex]>>,
    current_batch: Mutex<Vec<Tuple>>,
    current_batch_index: AtomicUsize,

    // mutex here just to make `Self: Sync`
    stream: OnceCell<Mutex<TupleStream>>,
}

type TupleStream = Pin<Box<dyn Stream<Item = nsql_buffer::Result<Vec<Tuple>>> + Send>>;

impl fmt::Debug for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan")
            .field("table_ref", &self.table_ref)
            .field("projection", &self.projection)
            .finish()
    }
}

impl PhysicalTableScan {
    pub(crate) fn plan(
        table_ref: ir::TableRef,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Arc<dyn PhysicalNode> {
        Arc::new(Self {
            table_ref,
            projection,
            table: Default::default(),
            current_batch: Default::default(),
            current_batch_index: Default::default(),
            stream: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalTableScan {
    #[tracing::instrument(skip(self, ctx))]
    async fn source(&self, ctx: &ExecutionContext) -> ExecutionResult<Chunk> {
        let tx = ctx.tx();
        let table = self.table.get_or_try_init(|| {
            let namespace = ctx.catalog.get(&tx, self.table_ref.namespace).unwrap();
            Ok::<_, nsql_catalog::Error>(namespace.get(&tx, self.table_ref.table).unwrap())
        })?;

        // FIXME we can return an entire chunk at a time instead now
        loop {
            let mut next_batch = self.current_batch.lock().await;
            let idx = self.current_batch_index.fetch_add(1, atomic::Ordering::AcqRel);
            if idx < next_batch.len() {
                let tuple = next_batch[idx].clone();
                tracing::debug!(%tuple, "returning tuple");
                return Ok(Chunk::singleton(tuple));
            } else {
                let stream = self
                    .stream
                    .get_or_init(|| async move {
                        let storage = table.storage();
                        let projection = self.projection.as_ref().map(|p| {
                            p.iter().map(|&idx| TupleIndex::new(idx.as_usize())).collect()
                        });
                        Mutex::new(Box::pin(storage.scan(ctx.tx(), projection).await) as _)
                    })
                    .await;
                let mut stream = stream.lock().await;
                match stream.next().await {
                    Some(batch) => {
                        let batch = batch.map_err(|report| report.into_error())?;
                        *next_batch = batch;
                        self.current_batch_index.store(0, atomic::Ordering::Release);
                    }
                    None => return Ok(Chunk::empty()),
                }
            }
        }
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

impl Explain for PhysicalTableScan {
    fn explain(
        &self,
        catalog: &Catalog,
        tx: &Transaction,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        // In this context, we know the projection indices correspond to the column indices of the source table
        let table = self.table_ref.get(catalog, tx);
        let columns = table.all::<Column>(tx);

        let column_names = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|&idx| {
                    columns
                        .get(idx.as_usize())
                        .map(|(_, col)| col.name())
                        // FIXME centralize this logic
                        .unwrap_or_else(|| "tid".into())
                })
                .collect::<Vec<_>>(),
            None => columns.iter().map(|(_, col)| col.name()).collect::<Vec<_>>(),
        };

        write!(f, "scan {} ({})", table.name(), column_names.iter().join(", "))?;
        Ok(())
    }
}
