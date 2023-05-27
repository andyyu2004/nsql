use std::sync::atomic::{self, AtomicUsize};
use std::sync::OnceLock;

use itertools::Itertools;
use nsql_catalog::{Column, ColumnIndex, Container, Entity, Table};
use nsql_storage::tuple::TupleIndex;
use nsql_storage_engine::FallibleIterator;
use parking_lot::Mutex;

use super::*;

pub struct PhysicalTableScan<S: StorageEngine> {
    table_ref: ir::TableRef<S>,
    table: OnceLock<Arc<Table<S>>>,
    projection: Option<Box<[ColumnIndex]>>,
    current_batch: Mutex<Vec<Tuple>>,
    current_batch_index: AtomicUsize,

    stream: OnceLock<Mutex<TupleStream<S>>>,
}

type TupleStream<S> =
    Box<dyn FallibleIterator<Item = Tuple, Error = <S as StorageEngine>::Error> + Send + Sync>;

impl<'env, S: StorageEngine> fmt::Debug for PhysicalTableScan<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan")
            .field("table_ref", &self.table_ref)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<'env, S: StorageEngine> PhysicalTableScan<S> {
    pub(crate) fn plan<M: ExecutionMode<'env, S>>(
        table_ref: ir::TableRef<S>,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Arc<dyn PhysicalNode<'env, S, M>> {
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
impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, S, M>
    for PhysicalTableScan<S>
{
    #[tracing::instrument(skip(self, ctx))]
    fn source(&self, ctx: &ExecutionContext<'env, S, M>) -> ExecutionResult<SourceState<Chunk>> {
        let table = self.table.get_or_try_init(|| {
            let tx = ctx.tx();
            let namespace = ctx.catalog.get(&**tx, self.table_ref.namespace).unwrap();
            Ok::<_, nsql_catalog::Error>(namespace.get(&**tx, self.table_ref.table).unwrap())
        })?;

        let stream = self.stream.get_or_try_init(|| {
            let storage = table.storage();
            let projection = self
                .projection
                .as_ref()
                .map(|p| p.iter().map(|&idx| TupleIndex::new(idx.as_usize())).collect());

            storage.scan(&**ctx.tx(), projection).map(|iter| Box::new(iter) as _).map(Mutex::new)
        })?;

        match stream.lock().next()? {
            Some(tuple) => return Ok(SourceState::Yield(Chunk::singleton(tuple))),
            None => return Ok(SourceState::Done),
        }
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, S, M>
    for PhysicalTableScan<S>
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

impl<'env, S: StorageEngine> Explain<S> for PhysicalTableScan<S> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
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
