use nsql_storage_engine::StorageEngine;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, Name, Table};

#[derive(Debug)]
pub struct Namespace<S> {
    name: Name,
    pub(crate) tables: CatalogSet<S, Table<S>>,
}

pub trait NamespaceEntity<S: StorageEngine>: CatalogEntity<S, Container = Namespace<S>> {}

impl<S: StorageEngine, T: CatalogEntity<S, Container = Namespace<S>>> NamespaceEntity<S> for T {}

impl<S: StorageEngine> Container<S> for Namespace<S> {}

#[derive(Debug)]
pub struct CreateNamespaceInfo {
    pub name: Name,
}

impl<S: StorageEngine> CatalogEntity<S> for Namespace<S> {
    type Container = Catalog<S>;

    type CreateInfo = CreateNamespaceInfo;

    #[inline]
    fn catalog_set(catalog: &Catalog<S>) -> &CatalogSet<S, Self> {
        &catalog.schemas
    }

    #[inline]
    fn create(_tx: &S::WriteTransaction<'_>, info: Self::CreateInfo) -> Self {
        Self { name: info.name, tables: Default::default() }
    }
}

impl<S: StorageEngine> Entity for Namespace<S> {
    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        // we still call this a "schema" in the sql world, but not internally to avoid confusion
        // with the other schema
        "schema"
    }
}
