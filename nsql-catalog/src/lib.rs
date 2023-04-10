#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod entry;
mod namespace;
mod set;
mod table;

use std::sync::Arc;

use nsql_core::Name;
use nsql_transaction::Transaction;
use thiserror::Error;

pub use self::entry::Oid;
pub use self::namespace::{CreateNamespaceInfo, Namespace, NamespaceEntity};
use self::private::CatalogEntity;
use self::set::CatalogSet;
pub use self::table::{CreateColumnInfo, CreateTableInfo, Table};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {}

#[derive(Debug)]
pub struct Catalog {
    schemas: CatalogSet<Namespace>,
}

pub const DEFAULT_SCHEMA: &str = "main";

impl Catalog {
    /// Create a blank catalog with the default schema
    pub fn create(tx: &Transaction) -> Result<Self> {
        let catalog = Self { schemas: Default::default() };
        catalog.create::<Namespace>(tx, CreateNamespaceInfo { name: DEFAULT_SCHEMA.into() })?;
        Ok(catalog)
    }
}

impl Container for Catalog {}

pub trait Entity {
    fn desc() -> &'static str;

    fn name(&self) -> &Name;
}

pub trait Container {
    fn create<T: CatalogEntity<Container = Self>>(
        &self,
        tx: &Transaction,
        info: T::CreateInfo,
    ) -> Result<()> {
        T::new(info).insert(self, tx);
        Ok(())
    }

    fn get<T: CatalogEntity<Container = Self>>(
        &self,
        tx: &Transaction,
        oid: Oid<T>,
    ) -> Result<Option<Arc<T>>> {
        Ok(T::get(self, tx, oid))
    }

    fn get_by_name<T: CatalogEntity<Container = Self>>(
        &self,
        tx: &Transaction,
        name: impl AsRef<str>,
    ) -> Result<Option<(Oid<T>, Arc<T>)>> {
        Ok(T::get_by_name(self, tx, name.as_ref()))
    }

    fn find<T: CatalogEntity<Container = Self>>(&self, name: &str) -> Result<Option<Oid<T>>> {
        Ok(T::find(self, name))
    }

    fn all<'a, T: CatalogEntity<Container = Self>>(
        &'a self,
        tx: &'a Transaction,
    ) -> Result<Vec<Arc<T>>> {
        Ok(T::all(self, tx))
    }
}

pub(crate) mod private {
    use nsql_serde::StreamSerialize;

    use super::*;

    /// This trait is sealed and cannot be implemented for types outside of this crate.
    /// These method should also not be visible to users of this crate.
    pub trait CatalogEntity: Entity + StreamSerialize + Sized {
        type Container;

        type CreateInfo;

        /// extract the `CatalogSet` from the `container` for `Self`
        fn catalog_set(container: &Self::Container) -> &CatalogSet<Self>;

        fn new(info: Self::CreateInfo) -> Self;

        #[inline]
        fn insert(self, container: &Self::Container, tx: &Transaction) -> Oid<Self> {
            Self::catalog_set(container).insert(tx, self)
        }

        #[inline]
        fn get(container: &Self::Container, tx: &Transaction, oid: Oid<Self>) -> Option<Arc<Self>> {
            Self::catalog_set(container).get(tx, oid)
        }

        #[inline]
        fn get_by_name(
            container: &Self::Container,
            tx: &Transaction,
            name: &str,
        ) -> Option<(Oid<Self>, Arc<Self>)> {
            Self::catalog_set(container).get_by_name(tx, name)
        }

        #[inline]
        fn find(container: &Self::Container, name: &str) -> Option<Oid<Self>> {
            Self::catalog_set(container).find(name)
        }

        #[inline]
        fn all(container: &Self::Container, tx: &Transaction) -> Vec<Arc<Self>> {
            Self::catalog_set(container).entries(tx)
        }
    }
}
