use itertools::Itertools;
use nsql_catalog::{Column, ColumnIndex, Table};
use nsql_storage::tuple::TupleIndex;
use nsql_storage_engine::FallibleIterator;

use super::*;

pub struct PhysicalTableScan<'env, 'txn, S, M> {
    id: PhysicalNodeId<'env, 'txn, S, M>,
    table: Table,
    columns: Box<[Column]>,
    projection: Option<Box<[ColumnIndex]>>,
}

impl<'env, 'txn, S, M> fmt::Debug for PhysicalTableScan<'env, 'txn, S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan")
            .field("table", &self.table)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalTableScan<'env, 'txn, S, M>
{
    pub(crate) fn plan(
        table: Table,
        columns: impl Into<Box<[Column]>>,
        projection: Option<Box<[ColumnIndex]>>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> PhysicalNodeId<'env, 'txn, S, M> {
        arena.alloc_with(|id| Arc::new(Self { id, table, columns: columns.into(), projection }))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalTableScan<'env, 'txn, S, M>
{
    #[tracing::instrument(skip(self, ecx))]
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tx = ecx.tx();
        let catalog = ecx.catalog();
        let storage = Arc::new(self.table.storage::<S, M>(catalog, tx)?);

        let projection = self
            .projection
            .as_ref()
            .map(|p| p.iter().map(|&idx| TupleIndex::new(idx.as_usize())).collect());

        let stream = storage.scan_arc(projection)?.map_err(Into::into);
        Ok(Box::new(stream) as _)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalTableScan<'env, 'txn, S, M>
{
    fn id(&self) -> PhysicalNodeId<'env, 'txn, S, M> {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        self.projection.as_ref().map_or_else(|| self.columns.len(), |p| p.len())
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        &[]
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalTableScan<'env, '_, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        // In this context, we know the projection indices correspond to the column indices of the source table
        let table = &self.table;
        let columns = table.columns(catalog, tx)?;

        let column_names = match &self.projection {
            Some(projection) => {
                projection.iter().map(|&idx| columns[idx.as_usize()].name()).collect::<Vec<_>>()
            }
            None => columns.iter().map(|col| col.name()).collect::<Vec<_>>(),
        };

        write!(f, "scan {} ({})", table.name(), column_names.iter().join(", "))?;
        Ok(())
    }
}
