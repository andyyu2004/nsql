use nsql_serde::{StreamDeserialize, StreamSerialize};
use nsql_storage::Transaction;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, Name, Table};

#[derive(Debug, StreamSerialize)]
pub struct Namespace {
    name: Name,
    pub(crate) tables: CatalogSet<Table>,
}

pub trait NamespaceEntity: CatalogEntity<Container = Namespace> {}

impl<T: CatalogEntity<Container = Namespace>> NamespaceEntity for T {}

impl Container for Namespace {}

#[derive(Debug, StreamSerialize, StreamDeserialize)]
pub struct CreateNamespaceInfo {
    pub name: Name,
}

impl CatalogEntity for Namespace {
    type Container = Catalog;

    type CreateInfo = CreateNamespaceInfo;

    #[inline]
    fn catalog_set(catalog: &Catalog) -> &CatalogSet<Self> {
        &catalog.schemas
    }

    #[inline]
    fn new(_tx: &Transaction, info: Self::CreateInfo) -> Self {
        Self { name: info.name, tables: Default::default() }
    }
}

impl Entity for Namespace {
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
