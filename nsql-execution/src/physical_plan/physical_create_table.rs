use nsql_catalog::{Column, Container, CreateTableInfo, Namespace, Table, TableRef};
use nsql_core::Name;
use nsql_storage::{TableStorage, TableStorageInfo};
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::{ReadWriteExecutionMode, TupleStream};

pub struct PhysicalCreateTable<S> {
    info: ir::CreateTableInfo<S>,
}

impl<S> fmt::Debug for PhysicalCreateTable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalCreateTable").field("info", &self.info).finish()
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalCreateTable<S> {
    pub(crate) fn plan(
        info: ir::CreateTableInfo<S>,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>> {
        Arc::new(Self { info })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalCreateTable<S>
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
    for PhysicalCreateTable<S>
{
    fn source(
        self: Arc<Self>,
        ctx: &'txn ExecutionContext<'env, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'txn, S>> {
        let catalog = ctx.catalog();
        let tx = ctx.tx()?;
        let namespace: Arc<Namespace<S>> = catalog
            .get::<Namespace<S>>(tx, self.info.namespace)
            .expect("schema not found during execution");

        let info = CreateTableInfo { name: self.info.name.clone() };

        let table_oid = namespace.create::<Table<S>>(tx, info)?;
        let table: Arc<Table<S>> =
            namespace.get::<Table<S>>(tx, table_oid).expect("table not found during execution");

        for info in &self.info.columns {
            table.create::<Column<S>>(tx, info.clone())?;
        }

        TableStorage::create(
            ctx.storage(),
            tx,
            TableStorageInfo::new(
                Name::from(format!(
                    "{}",
                    TableRef { namespace: self.info.namespace, table: table_oid }
                )),
                table.columns(tx).iter().map(|c| c.as_ref().into()).collect(),
            ),
        )?;

        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalCreateTable<S> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &dyn Transaction<'_, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "create table {}", self.info.name)?;
        Ok(())
    }
}
