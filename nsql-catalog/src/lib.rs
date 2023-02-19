#![feature(never_type)]

mod entry;
mod schema;
mod set;

use nsql_transaction::Transaction;
use parking_lot::RwLock;

use self::entry::EntryName;
pub use self::schema::Schema;
use self::set::{CatalogEntry, CatalogSet};

pub type Result<T, E = !> = std::result::Result<T, E>;

pub struct Catalog {
    schemas: RwLock<CatalogSet<Schema>>,
}

trait CatalogEntity {
    fn name(&self) -> &EntryName;
}

impl Catalog {
    pub fn schemas(&self, tx: &Transaction) -> Result<Vec<CatalogEntry<Schema>>> {
        Ok(self.schemas.read().entries(tx).cloned().collect())
    }

    pub fn schema(&self, tx: &Transaction, name: &str) -> Result<Option<CatalogEntry<Schema>>> {
        Ok(self.schemas.read().find(tx, name).cloned())
    }

    pub fn create_schema(&self, tx: &Transaction, name: impl Into<EntryName>) -> Result<()> {
        self.schemas.write().insert(tx, Schema::new(name.into(), false));
        Ok(())
    }

    pub fn create_internal_schema(
        &self,
        tx: &Transaction,
        name: impl Into<EntryName>,
    ) -> Result<()> {
        self.schemas.write().insert(tx, Schema::new(name.into(), true));
        Ok(())
    }
}
