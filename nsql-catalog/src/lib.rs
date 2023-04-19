#![feature(async_fn_in_trait)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod entity;
mod entry;
mod set;

use std::sync::Arc;

pub use anyhow::Error;
use nsql_core::Name;
pub use nsql_transaction::Transaction;

pub use self::entity::namespace::{CreateNamespaceInfo, Namespace, NamespaceEntity};
pub use self::entity::table::{Column, CreateColumnInfo, CreateTableInfo, Table};
pub use self::entry::Oid;
use self::private::CatalogEntity;
use self::set::{AlreadyExists, CatalogSet};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Catalog {
    schemas: CatalogSet<Namespace>,
}

pub const DEFAULT_SCHEMA: &str = "main";

impl Catalog {
    /// Create a blank catalog with the default schema
    pub fn create(tx: &Transaction) -> Result<Self> {
        let catalog = Self { schemas: Default::default() };
        catalog
            .create::<Namespace>(tx, CreateNamespaceInfo { name: DEFAULT_SCHEMA.into() })
            .expect("default schema should not already exist");
        Ok(catalog)
    }
}

impl Container for Catalog {}

pub trait Entity {
    fn name(&self) -> Name;

    fn desc() -> &'static str;
}

pub trait Container {
    fn create<T: CatalogEntity<Container = Self>>(
        &self,
        tx: &Transaction,
        info: T::CreateInfo,
    ) -> Result<(), AlreadyExists<T>> {
        T::new(tx, info).insert(self, tx)?;
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
    ) -> Result<Vec<(Oid<T>, Arc<T>)>> {
        Ok(T::all(self, tx))
    }
}

pub(crate) mod private {
    use nsql_serde::StreamSerialize;

    use super::*;
    use crate::set::AlreadyExists;

    /// This trait is sealed and cannot be implemented for types outside of this crate.
    /// These method should also not be visible to users of this crate.
    pub trait CatalogEntity: Entity + StreamSerialize + Sized {
        type Container;

        type CreateInfo;

        /// extract the `CatalogSet` from the `container` for `Self`
        fn catalog_set(container: &Self::Container) -> &CatalogSet<Self>;

        fn new(tx: &Transaction, info: Self::CreateInfo) -> Self;

        #[inline]
        fn insert(
            self,
            container: &Self::Container,
            tx: &Transaction,
        ) -> Result<Oid<Self>, AlreadyExists<Self>> {
            Self::catalog_set(container).insert(tx, self)
        }

        #[inline]
        fn try_insert(
            self,
            container: &Self::Container,
            tx: &Transaction,
        ) -> Result<Oid<Self>, AlreadyExists<Self>> {
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
        fn all(container: &Self::Container, tx: &Transaction) -> Vec<(Oid<Self>, Arc<Self>)> {
            Self::catalog_set(container).entries(tx)
        }
    }
}
