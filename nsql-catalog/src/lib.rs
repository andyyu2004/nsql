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
pub use self::schema::Schema;
use self::set::CatalogSet;

pub type Result<T, E = !> = std::result::Result<T, E>;

pub struct Catalog {
    schemas: RwLock<CatalogSet<Schema>>,
}

trait CatalogEntity: Serialize + Deserialize {
    fn name(&self) -> &EntryName;
}

impl Catalog {
    pub fn schemas(&self, tx: &Transaction) -> Result<Vec<Arc<Schema>>> {
        Ok(self.schemas.read().entries(tx).collect())
    }

    pub fn schema(&self, tx: &Transaction, name: &str) -> Result<Option<Arc<Schema>>> {
        Ok(self.schemas.read().find(tx, name))
    }

    pub fn create_schema(&self, tx: &Transaction, name: impl Into<EntryName>) -> Result<()> {
        self.schemas.write().insert(tx, Schema::new(name.into()));
        Ok(())
    }
}
