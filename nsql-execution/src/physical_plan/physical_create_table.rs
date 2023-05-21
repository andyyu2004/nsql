use nsql_catalog::{Column, Container, CreateTableInfo, Namespace, Table};
use nsql_storage::schema::{Attribute, Schema};
use nsql_storage::{TableStorage, TableStorageInfo};

use super::*;
use crate::ReadWriteExecutionMode;

pub struct PhysicalCreateTable<S> {
    info: ir::CreateTableInfo<S>,
}

impl<S> fmt::Debug for PhysicalCreateTable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalCreateTable").field("info", &self.info).finish()
    }
}

impl<'env, S: StorageEngine> PhysicalCreateTable<S> {
    pub(crate) fn plan(
        info: ir::CreateTableInfo<S>,
    ) -> Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>> {
        Arc::new(Self { info })
    }
}

impl<'env, S: StorageEngine> PhysicalNode<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalCreateTable<S>
{
    #[inline]
    fn children(&self) -> &[Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>] {
        &[]
    }

    #[inline]
    fn as_source(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSource<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Ok(self)
    }

    #[inline]
    fn as_sink(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalSink<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }

    #[inline]
    fn as_operator(
        self: Arc<Self>,
    ) -> Result<
        Arc<dyn PhysicalOperator<'env, S, ReadWriteExecutionMode<S>>>,
        Arc<dyn PhysicalNode<'env, S, ReadWriteExecutionMode<S>>>,
    > {
        Err(self)
    }
}

impl<'env, S: StorageEngine> PhysicalSource<'env, S, ReadWriteExecutionMode<S>>
    for PhysicalCreateTable<S>
{
    fn source(
        &self,
        ctx: &ExecutionContext<'env, S, ReadWriteExecutionMode<S>>,
    ) -> ExecutionResult<SourceState<Chunk>> {
        let attrs = self
            .info
            .columns
            .iter()
            .map(|c| Attribute::new(c.name.clone(), c.ty.clone()))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(attrs));

        let info = CreateTableInfo {
            name: self.info.name.clone(),
            storage: Arc::new(TableStorage::initialize(
                ctx.storage(),
                TableStorageInfo::create(schema),
            )?),
        };

        let catalog = ctx.catalog();
        let mut tx = ctx.tx_mut();
        let schema = catalog
            .get::<Namespace<S>>(&*tx, self.info.namespace)
            .expect("schema not found during execution");

        let table_oid = schema.create::<Table<S>>(&mut tx, info)?;
        let table =
            schema.get::<Table<S>>(&*tx, table_oid).expect("table not found during execution");
        for info in &self.info.columns {
            table.create::<Column>(&mut tx, info.clone())?;
        }

        Ok(SourceState::Done)
    }
}

impl<'env, S: StorageEngine> Explain<S> for PhysicalCreateTable<S> {
    fn explain(
        &self,
        _catalog: &Catalog<S>,
        _tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "create table {}", self.info.name)?;
        Ok(())
    }
}
