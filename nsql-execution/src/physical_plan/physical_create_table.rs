use nsql_catalog::{Column, ColumnIndex, SystemEntity, Table, Type};
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

pub struct PhysicalCreateTable {
    info: ir::CreateTableInfo,
}

impl fmt::Debug for PhysicalCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalCreateTable").field("info", &self.info).finish()
    }
}

impl<'env: 'txn, 'txn> PhysicalCreateTable {
    pub(crate) fn plan<S: StorageEngine>(
        info: ir::CreateTableInfo,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self { info })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalCreateTable
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, 'txn, S, ReadWriteExecutionMode>>,
        Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>>,
    > {
        Err(self)
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalCreateTable
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        tracing::debug!(name = %self.info.name, "physical create table");
        assert!(!self.info.columns.is_empty());

        let catalog = ctx.catalog();

        let tx = ctx.tx()?;

        let table = Table::new(self.info.namespace, self.info.name.clone());

        catalog.system_table_write(tx)?.insert(table.clone())?;

        let mut columns = catalog.system_table_write(tx)?;
        for info in &self.info.columns {
            columns.insert(Column::new(
                table.oid(),
                info.name.clone(),
                ColumnIndex::new(info.index),
                Type::logical_type_to_oid(&info.ty),
                info.is_primary_key,
            ))?;
        }

        // must drop the columns before the next line otherwise the next line will fail as it will
        // try to open the `columns` table again
        drop(columns);

        // this must be called after creating the columns
        table.get_or_create_storage(catalog, tx)?;

        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<S: StorageEngine> Explain<'_, S> for PhysicalCreateTable {
    fn explain(
        &self,
        _catalog: Catalog<'_, S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "create table {}", self.info.name)?;
        Ok(())
    }
}
