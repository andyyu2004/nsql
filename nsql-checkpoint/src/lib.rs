use std::io;

use nsql_catalog::{Catalog, Container, Namespace, Table};
use nsql_pager::{MetaPageReader, MetaPageWriter, Pager};
use nsql_serde::Serialize;
use nsql_transaction::Transaction;
use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),
}

impl From<error_stack::Report<io::Error>> for Error {
    fn from(e: error_stack::Report<io::Error>) -> Self {
        Self::Io(Box::new(e.into_error()))
    }
}

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
        let data_page = self.pager.alloc_page().await?;

        let meta_writer = MetaPageWriter::new(self.pager, meta_page);
        let data_writer = MetaPageWriter::new(self.pager, data_page);

        let mut writer = CheckpointWriter::new(meta_writer, data_writer);
        writer.write_catalog(tx, catalog).await?;
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
        _tx: &Transaction,
        _reader: &mut MetaPageReader<'_, P>,
    ) -> Result<Catalog> {
        todo!();
        // let catalog = Catalog::deserialize_with(tx, reader).await?;
        // Ok(catalog)
    }
}

struct CheckpointWriter<'a, P> {
    meta_writer: MetaPageWriter<'a, P>,
    data_writer: MetaPageWriter<'a, P>,
}

impl<'a, P: Pager> CheckpointWriter<'a, P> {
    fn new(meta_writer: MetaPageWriter<'a, P>, data_writer: MetaPageWriter<'a, P>) -> Self {
        Self { meta_writer, data_writer }
    }

    async fn write_catalog(&mut self, tx: &Transaction, catalog: &Catalog) -> Result<()> {
        for schema in catalog.all::<Namespace>(tx)? {
            self.write_schema(tx, &schema).await?;
        }
        Ok(())
    }

    async fn write_schema(&mut self, tx: &Transaction, schema: &Namespace) -> Result<()> {
        schema.serialize(&mut self.meta_writer).await?;
        for table in schema.all::<Table>(tx)? {
            self.write_table_data(tx, &table).await?;
        }
        Ok(())
    }

    async fn write_table_data(&self, _tx: &Transaction, _table: &Table) -> Result<()> {
        todo!()
    }
}
