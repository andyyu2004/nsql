use nsql_catalog::{Catalog, CreateSchemaInfo};
use nsql_pager::{MetaPageReader, MetaPageWriter, Pager};
use nsql_serde::{Deserialize, Serialize};
use nsql_transaction::Transaction;

use crate::Result;

pub struct Checkpointer<'a, P> {
    pager: &'a P,
}

impl<'a, P> Checkpointer<'a, P> {
    pub fn new(pager: &'a P) -> Self {
        Self { pager }
    }
}

pub struct Checkpoint {
    pub catalog: Catalog,
}

impl<P: Pager> Checkpointer<'_, P> {
    pub async fn checkpoint(&self, tx: &Transaction, catalog: &Catalog) -> Result<()> {
        let meta_page = self.pager.alloc_page().await?;
        let mut writer = MetaPageWriter::new(self.pager, meta_page);

        let schemas = match catalog.schemas(tx) {
            Ok(schemas) => schemas,
            Err(_) => todo!(),
        };

        schemas.serialize(&mut writer).await?;
        Ok(())
    }

    pub async fn load_checkpoint(
        &self,
        tx: &Transaction,
        mut reader: MetaPageReader<'_, P>,
    ) -> Result<Checkpoint> {
        let catalog = self.load_catalog(tx, &mut reader).await?;
        Ok(Checkpoint { catalog })
    }

    async fn load_catalog(
        &self,
        tx: &Transaction,
        reader: &mut MetaPageReader<'_, P>,
    ) -> Result<Catalog> {
        let catalog = Catalog::default();
        let schemas = Vec::<CreateSchemaInfo>::deserialize(reader).await?;
        for schema in schemas {
            catalog.create_schema(tx, schema)?;
        }
        Ok(catalog)
    }
}
