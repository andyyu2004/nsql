use std::fmt;
use std::sync::Arc;

use nsql_serde::{Deserialize, Serialize};
use nsql_storage::tuple::{AttributeSpec, PhysicalType};
use nsql_storage::{tuple, TableStorage};

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, LogicalType, Name, Namespace};

#[derive(Clone, Serialize)]
pub struct Table {
    name: Name,
    #[serde(skip)]
    storage: Arc<TableStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    attributes: Vec<Attribute>,
}

impl tuple::Schema for Schema {
    fn attributes(&self) -> Box<dyn ExactSizeIterator<Item = &dyn AttributeSpec> + '_> {
        Box::new(self.attributes.iter().map(|attr| attr as &dyn AttributeSpec))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    name: Name,
    logical_type: LogicalType,
    cached_physical_type: PhysicalType,
}

impl Attribute {
    pub fn new(name: Name, logical_type: LogicalType) -> Self {
        Self { name, cached_physical_type: (&logical_type).into(), logical_type }
    }
}

impl AttributeSpec for Attribute {
    fn physical_type(&self) -> &PhysicalType {
        &self.cached_physical_type
    }
}

impl Table {
    #[inline]
    pub fn storage(&self) -> &TableStorage {
        self.storage.as_ref()
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct CreateTableInfo {
    pub name: Name,
    pub columns: Vec<CreateColumnInfo>,
    pub storage: Arc<TableStorage>,
}

impl fmt::Debug for CreateTableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTableInfo")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateColumnInfo {
    pub name: Name,
    pub ty: LogicalType,
}

impl Entity for Table {
    fn name(&self) -> &Name {
        &self.name
    }

    fn desc() -> &'static str {
        "table"
    }
}

impl CatalogEntity for Table {
    type Container = Namespace;

    type CreateInfo = CreateTableInfo;

    fn catalog_set(container: &Self::Container) -> &CatalogSet<Self> {
        &container.tables
    }

    fn new(info: Self::CreateInfo) -> Self {
        Self { name: info.name, storage: info.storage }
    }
}
