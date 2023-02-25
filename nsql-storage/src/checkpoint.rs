use nsql_catalog::{Catalog, Container, CreateSchemaInfo, Schema};
use nsql_pager::{MetaPageReader, MetaPageWriter, Pager};
use nsql_serde::{Deserialize, DeserializeWith, Serialize};
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
        let mut serializer = MetaPageWriter::new(self.pager, meta_page);
        catalog.serialize(&mut serializer).await?;
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
        let catalog = Catalog::deserialize_with(tx, reader).await?;
        Ok(catalog)
    }
}
