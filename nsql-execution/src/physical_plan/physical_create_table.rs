use nsql_catalog::{Column, Container, CreateTableInfo, Namespace, Table};
use nsql_storage::schema::{Attribute, Schema};
use nsql_storage::{TableStorage, TableStorageInfo};

use super::*;

pub struct PhysicalCreateTable<S> {
    info: ir::CreateTableInfo<S>,
}

impl<S> fmt::Debug for PhysicalCreateTable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PhysicalCreateTable").field("info", &self.info).finish()
    }
}

impl<S: StorageEngine> PhysicalCreateTable<S> {
    pub(crate) fn plan(info: ir::CreateTableInfo<S>) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { info })
    }
}

impl<S: StorageEngine> PhysicalNode<S> for PhysicalCreateTable<S> {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl<S: StorageEngine> PhysicalSource<S> for PhysicalCreateTable<S> {
    async fn source(&self, ctx: &ExecutionContext<'_, S>) -> ExecutionResult<SourceState<Chunk>> {
        let attrs = self
            .info
            .columns
            .iter()
            .map(|c| Attribute::new(c.name.clone(), c.ty.clone()))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(attrs));

        let info = CreateTableInfo {
            name: self.info.name.clone(),
            storage: Arc::new(
                TableStorage::initialize(ctx.storage(), TableStorageInfo::create(schema))
                    .await
                    .map_err(|report| report.into_error())?,
            ),
        };

        let catalog = ctx.catalog();
        let tx = ctx.tx();
        let schema = catalog
            .get::<Namespace<S>>(&tx, self.info.namespace)
            .expect("schema not found during execution");

        let table_oid = schema.create::<Table<S>>(&tx, info)?;
        let table =
            schema.get::<Table<S>>(&tx, table_oid).expect("table not found during execution");
        for info in &self.info.columns {
            table.create::<Column>(&tx, info.clone())?;
        }

        Ok(SourceState::Done)
    }
}

impl<S: StorageEngine> Explain<S> for PhysicalCreateTable<S> {
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
