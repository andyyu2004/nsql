#![feature(never_type)]
#![feature(type_alias_impl_trait)]
#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod entry;
mod schema;
mod set;
mod table;

use std::sync::Arc;

use nsql_serde::{Deserialize, Serialize};
use nsql_transaction::Transaction;
use parking_lot::RwLock;
use thiserror::Error;

use self::entry::{EntryName, Oid};
pub use self::schema::{CreateSchemaInfo, Schema};
use self::set::CatalogSet;
pub use self::table::{CreateTableInfo, Table};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {}

#[derive(Default)]
pub struct Catalog {
    schemas: RwLock<CatalogSet<Schema>>,
    tables: RwLock<CatalogSet<Table>>,
}

pub trait CatalogEntity: private::Sealed + Serialize {
    type CreateInfo: Deserialize;

    fn new(info: Self::CreateInfo) -> Self;

    fn name(&self) -> &EntryName;

    #[inline]
    fn insert(self, catalog: &Catalog, tx: &Transaction) -> Oid<Self> {
        Self::catalog_set(catalog).write().insert(tx, self)
    }

    #[inline]
    fn get(catalog: &Catalog, tx: &Transaction, oid: Oid<Self>) -> Option<Arc<Self>> {
        Self::catalog_set(catalog).read().get(tx, oid)
    }

    #[inline]
    fn find(catalog: &Catalog, tx: &Transaction, name: &str) -> Option<Oid<Self>> {
        Self::catalog_set(catalog).read().find(tx, name)
    }

    #[inline]
    fn all<'a>(catalog: &'a Catalog, tx: &'a Transaction) -> Vec<Arc<Self>> {
        Self::catalog_set(catalog).read().entries(tx).collect()
    }
}

pub(crate) mod private {
    use parking_lot::RwLock;

    use crate::set::CatalogSet;
    use crate::Catalog;

    pub trait Sealed: Sized {
        fn catalog_set(catalog: &Catalog) -> &RwLock<CatalogSet<Self>>;
    }
}

impl Catalog {
    pub fn create<T: CatalogEntity>(&self, tx: &Transaction, info: T::CreateInfo) -> Result<()> {
        T::new(info).insert(self, tx);
        Ok(())
    }

    pub fn get<T: CatalogEntity>(&self, tx: &Transaction, oid: Oid<T>) -> Result<Option<Arc<T>>> {
        Ok(T::get(self, tx, oid))
    }

    pub fn find<T: CatalogEntity>(&self, tx: &Transaction, name: &str) -> Result<Option<Oid<T>>> {
        Ok(T::find(self, tx, name))
    }

    pub fn all<'a, T: CatalogEntity>(&'a self, tx: &'a Transaction) -> Result<Vec<Arc<T>>> {
        Ok(T::all(self, tx))
    }
}
