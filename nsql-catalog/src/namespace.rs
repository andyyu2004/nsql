use nsql_serde::{Deserialize, Deserializer, Serialize};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Catalog, Container, Entity, Name, Table};

#[derive(Debug, Serialize)]
pub struct Namespace {
    name: Name,
    pub(crate) tables: CatalogSet<Table>,
}

pub trait NamespaceEntity: CatalogEntity<Container = Namespace> {}

impl<T: CatalogEntity<Container = Namespace>> NamespaceEntity for T {}

impl Container for Namespace {}

#[derive(Debug)]
pub struct CreateNamespaceInfo {
    pub name: Name,
}

impl Deserialize for CreateNamespaceInfo {
    async fn deserialize(de: &mut dyn Deserializer) -> nsql_serde::Result<Self> {
        let s = de.read_str().await?;
        Ok(Self { name: Name::from(s.as_str()) })
    }
}

impl CatalogEntity for Namespace {
    type Container = Catalog;

    type CreateInfo = CreateNamespaceInfo;

    #[inline]
    fn catalog_set(catalog: &Catalog) -> &CatalogSet<Self> {
        &catalog.schemas
    }

    #[inline]
    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name, tables: Default::default() }
    }
}

impl Entity for Namespace {
    #[inline]
    fn desc() -> &'static str {
        // we still call this a "schema" in the sql world, but not internally to avoid confusion
        // with the other schema
        "schema"
    }

    #[inline]
    fn name(&self) -> &Name {
        &self.name
    }
}
