#![feature(never_type)]
#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod entry;
mod schema;
mod set;

use std::sync::Arc;

use nsql_serde::{Deserialize, Serialize};
use nsql_transaction::Transaction;
use parking_lot::RwLock;

use self::entry::EntryName;
pub use self::schema::{CreateSchemaInfo, Schema};
use self::set::CatalogSet;

pub type Result<T, E = std::io::Error> = std::result::Result<T, E>;

#[derive(Default)]
pub struct Catalog {
    schemas: RwLock<CatalogSet<Schema>>,
}

trait CatalogEntity: Serialize {
    type CreateInfo: Deserialize;

    fn new(info: Self::CreateInfo) -> Self;

    fn name(&self) -> &EntryName;
}

impl Catalog {
    #[tracing::instrument(skip(self))]
    pub fn schemas(&self, tx: &Transaction) -> Result<Vec<Arc<Schema>>> {
        Ok(self.schemas.read().entries(tx).collect())
    }

    #[tracing::instrument(skip(self))]
    pub fn schema(&self, tx: &Transaction, name: &str) -> Result<Option<Arc<Schema>>> {
        Ok(self.schemas.read().find(tx, name))
    }

    #[tracing::instrument(skip(self))]
    pub fn create_schema(&self, tx: &Transaction, info: CreateSchemaInfo) -> Result<()> {
        self.schemas.write().insert(tx, Schema::new(info));
        Ok(())
    }
}
