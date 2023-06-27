use itertools::Itertools;
use nsql_catalog::{ColumnIndex, Table};
use nsql_core::Oid;
use nsql_storage::tuple::TupleIndex;
use nsql_storage_engine::FallibleIterator;

use super::*;

pub struct PhysicalTableScan {
    schema: Schema,
    table: Oid<Table>,
    projection: Option<Box<[ColumnIndex]>>,
}

impl fmt::Debug for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan")
            .field("table", &self.table)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<'env: 'txn, 'txn> PhysicalTableScan {
    pub(crate) fn plan<S: StorageEngine, M: ExecutionMode<'env, S>>(
        schema: Schema,
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { schema, table, projection })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalTableScan
{
    #[tracing::instrument(skip(self, ecx))]
    fn source(
        self: Arc<Self>,
        ecx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        let tx = ecx.tx();
        let catalog = ecx.catalog();
        let table = catalog.table(&tx, self.table)?;
        let storage = Arc::new(table.storage::<S, M>(catalog, tx)?);

        let projection = self
            .projection
            .as_ref()
            .map(|p| p.iter().map(|&idx| TupleIndex::new(idx.as_usize())).collect());

        let stream = storage.scan_arc(projection)?.map_err(Into::into);
        Ok(Box::new(stream) as _)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalTableScan
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
        &[]
    }

    fn schema(&self) -> &[LogicalType] {
        &self.schema
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

impl<'env, S: StorageEngine> Explain<'env, S> for PhysicalTableScan {
    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        // In this context, we know the projection indices correspond to the column indices of the source table
        let table = catalog.table(tx, self.table)?;
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
