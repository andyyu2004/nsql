#![deny(rust_2018_idioms)]

mod bootstrap;
mod entity;
pub mod schema;
mod set;

use std::sync::Arc;

pub use anyhow::Error;
use nsql_core::{Name, Oid};
use nsql_storage_engine::{StorageEngine, Transaction};

pub use self::entity::namespace::{CreateNamespaceInfo, Namespace, NamespaceEntity};
pub use self::entity::table::{
    Column, ColumnIndex, ColumnRef, CreateColumnInfo, CreateTableInfo, Table, TableRef,
};
use self::private::CatalogEntity;
use self::set::{CatalogSet, Conflict};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Catalog<S> {
    schemas: CatalogSet<S, Namespace<S>>,
}

pub const DEFAULT_SCHEMA: &str = "main";

impl<S: StorageEngine> Catalog<S> {
    /// Create a blank catalog with the default schema
    pub fn create(storage: &S, tx: &S::WriteTransaction<'_>) -> Result<Self> {
        bootstrap::bootstrap(storage, tx)?;

        let catalog = Self { schemas: Default::default() };
        catalog
            .create::<Namespace<S>>(tx, CreateNamespaceInfo { name: DEFAULT_SCHEMA.into() })
            .expect("default schema should not already exist");
        Ok(catalog)
    }
}

impl<S: StorageEngine> Container<S> for Catalog<S> {}

pub trait Entity {
    fn oid(&self) -> Oid<Self>;

    fn name(&self) -> Name;

    fn desc() -> &'static str;
}

pub trait EntityRef<S: StorageEngine>: Copy {
    type Entity: CatalogEntity<S, Container = Self::Container>;

    type Container: Container<S>;

    fn container(self, catalog: &Catalog<S>, tx: &dyn Transaction<'_, S>) -> Arc<Self::Container>;

    fn entity_oid(self) -> Oid<Self::Entity>;

    fn get(self, catalog: &Catalog<S>, tx: &dyn Transaction<'_, S>) -> Arc<Self::Entity> {
        self.container(catalog, tx)
            .get(tx, self.entity_oid())
            .expect("`oid` should be valid for `tx`")
    }

    fn delete(self, catalog: &Catalog<S>, tx: &S::WriteTransaction<'_>) -> Result<()> {
        self.container(catalog, tx).delete(tx, self.entity_oid())?;
        Ok(())
    }
}

pub trait Container<S: StorageEngine> {
    fn create<T: CatalogEntity<S, Container = Self>>(
        &self,
        tx: &S::WriteTransaction<'_>,
        info: T::CreateInfo,
    ) -> Result<Oid<T>, Conflict<S, T>> {
        T::insert(self, tx, info)
    }

    fn get<T: CatalogEntity<S, Container = Self>>(
        &self,
        tx: &dyn Transaction<'_, S>,
        oid: Oid<T>,
    ) -> Option<Arc<T>> {
        T::get(self, tx, oid)
    }

    /// Delete the entity with the given `oid` from the catalog.
    /// Panics if the `oid` is not visible to `tx`.
    fn delete<T: CatalogEntity<S, Container = Self>>(
        &self,
        tx: &S::WriteTransaction<'_>,
        oid: Oid<T>,
    ) -> Result<(), Conflict<S, T>> {
        T::delete(self, tx, oid)
    }

    fn get_by_name<T: CatalogEntity<S, Container = Self>>(
        &self,
        tx: &dyn Transaction<'_, S>,
        name: impl AsRef<str>,
    ) -> Result<Option<(Oid<T>, Arc<T>)>> {
        Ok(T::get_by_name(self, tx, name.as_ref()))
    }

    fn find<T: CatalogEntity<S, Container = Self>>(&self, name: &str) -> Result<Option<Oid<T>>> {
        Ok(T::find(self, name))
    }

    fn all<T: CatalogEntity<S, Container = Self>>(
        &self,
        tx: &dyn Transaction<'_, S>,
    ) -> Vec<Arc<T>> {
        T::all(self, tx)
    }
}

pub(crate) mod private {

    use super::*;
    use crate::set::Conflict;

    /// This trait is sealed and cannot be implemented for types outside of this crate.
    /// These method should also not be visible to users of this crate.
    pub trait CatalogEntity<S: StorageEngine>: Entity + Send + Sync + Sized + 'static {
        type Container;

        type CreateInfo;

        /// extract the `CatalogSet` from the `container` for `Self`
        fn catalog_set(container: &Self::Container) -> &CatalogSet<S, Self>;

        fn create(
            tx: &S::WriteTransaction<'_>,
            container: &Self::Container,
            oid: Oid<Self>,
            info: Self::CreateInfo,
        ) -> Self;

        #[inline]
        fn insert(
            container: &Self::Container,
            tx: &S::WriteTransaction<'_>,
            info: Self::CreateInfo,
        ) -> Result<Oid<Self>, Conflict<S, Self>> {
            Self::catalog_set(container).insert(tx, container, info)
        }

        // #[inline]
        // fn try_insert(
        //     self,
        //     container: &Self::Container,
        //     tx: &S::WriteTransaction<'_>,
        // ) -> Result<Oid<Self>, Conflict<S, Self>> {
        //     Self::catalog_set(container).insert(tx, self)
        // }

        #[inline]
        fn get(
            container: &Self::Container,
            tx: &dyn Transaction<'_, S>,
            oid: Oid<Self>,
        ) -> Option<Arc<Self>> {
            Self::catalog_set(container).get(tx, oid)
        }

        #[inline]
        fn delete(
            container: &Self::Container,
            tx: &S::WriteTransaction<'_>,
            oid: Oid<Self>,
        ) -> Result<(), Conflict<S, Self>> {
            Self::catalog_set(container).delete(tx, oid)
        }

        #[inline]
        fn get_by_name(
            container: &Self::Container,
            tx: &dyn Transaction<'_, S>,
            name: &str,
        ) -> Option<(Oid<Self>, Arc<Self>)> {
            Self::catalog_set(container).get_by_name(tx, name)
        }

        #[inline]
        fn find(container: &Self::Container, name: &str) -> Option<Oid<Self>> {
            Self::catalog_set(container).find(name)
        }

        #[inline]
        fn all(container: &Self::Container, tx: &dyn Transaction<'_, S>) -> Vec<Arc<Self>> {
            Self::catalog_set(container).entries(tx)
        }
    }
}
