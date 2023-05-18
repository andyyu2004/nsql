use nsql_serde::{StreamDeserialize, StreamSerialize};
use nsql_storage::Transaction;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, Name, Table};

#[derive(Debug)]
pub struct Namespace<S> {
    name: Name,
    pub(crate) tables: CatalogSet<S, Table<S>>,
}

pub trait NamespaceEntity<S>: CatalogEntity<S, Container = Namespace<S>> {}

impl<S, T: CatalogEntity<S, Container = Namespace<S>>> NamespaceEntity<S> for T {}

impl<S> Container<S> for Namespace<S> {}

#[derive(Debug, StreamSerialize, StreamDeserialize)]
pub struct CreateNamespaceInfo {
    pub name: Name,
}

impl<S> CatalogEntity<S> for Namespace<S> {
    type Container = Catalog<S>;

    type CreateInfo = CreateNamespaceInfo;

    #[inline]
    fn catalog_set(catalog: &Catalog<S>) -> &CatalogSet<S, Self> {
        &catalog.schemas
    }

    #[inline]
    fn create(_tx: &Transaction, info: Self::CreateInfo) -> Self {
        Self { name: info.name, tables: Default::default() }
    }
}

impl<S> Entity for Namespace<S> {
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
