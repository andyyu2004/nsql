use std::sync::OnceLock;

use itertools::Itertools;
use nsql_catalog::{Column, ColumnIndex, Entity, Table, TableRef};
use nsql_storage::tuple::TupleIndex;

use super::*;

pub struct PhysicalTableScan {
    table_ref: TableRef,
    table: OnceLock<Arc<Table>>,
    projection: Option<Box<[ColumnIndex]>>,
}

impl fmt::Debug for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalTableScan")
            .field("table_ref", &self.table_ref)
            .field("projection", &self.projection)
            .finish()
    }
}

impl<'env: 'txn, 'txn> PhysicalTableScan {
    pub(crate) fn plan<S: StorageEngine, M: ExecutionMode<'env, S>>(
        table_ref: TableRef,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { table_ref, projection, table: Default::default() })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalTableScan
{
    #[tracing::instrument(skip(self, ctx))]
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        todo!();
        // let tx = ctx.tx()?;
        // let table = self.table.get_or_init(|| self.table_ref.get(&ctx.catalog, &tx));
        //
        // let storage = Arc::new(table.storage::<M>(ctx.storage(), tx)?);
        //
        // let projection = self
        //     .projection
        //     .as_ref()
        //     .map(|p| p.iter().map(|&idx| TupleIndex::new(idx.as_usize())).collect());
        //
        // let stream = storage.scan_arc(projection)?;
        // Ok(Box::new(stream) as _)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalTableScan
{
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, M>>] {
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

impl<S: StorageEngine> Explain<S> for PhysicalTableScan {
    fn explain(
        &self,
        catalog: &Catalog,
        tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        // In this context, we know the projection indices correspond to the column indices of the source table
        // let table = self.table_ref.get(catalog, tx);
        todo!();
        // let columns = table.all::<Column<S>>(tx);
        //
        // let column_names = match &self.projection {
        //     Some(projection) => projection
        //         .iter()
        //         .map(|&idx| {
        //             columns
        //                 .get(idx.as_usize())
        //                 .map(|col| col.name())
        //                 // FIXME centralize this logic
        //                 .unwrap_or_else(|| "tid".into())
        //         })
        //         .collect::<Vec<_>>(),
        //     None => columns.iter().map(|col| col.name()).collect::<Vec<_>>(),
        // };
        //
        // write!(f, "scan {} ({})", table.name(), column_names.iter().join(", "))?;
        // Ok(())
    }
}
