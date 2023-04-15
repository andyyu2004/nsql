use std::fmt;

use nsql_core::schema::LogicalType;
use nsql_serde::{StreamDeserialize, StreamSerialize};
use nsql_transaction::Transaction;

use crate::private::CatalogEntity;
use crate::set::CatalogSet;
use crate::{Entity, Name, Table};

#[derive(Clone, StreamSerialize)]
pub struct Column {
    name: Name,
    ty: LogicalType,
}

impl Column {
    #[inline]
    pub fn logical_type(&self) -> &LogicalType {
        &self.ty
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Column").field("name", &self.name).finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, StreamDeserialize)]
pub struct CreateColumnInfo {
    pub name: Name,
    pub ty: LogicalType,
}

impl Entity for Column {
    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "column"
    }
}

impl CatalogEntity for Column {
    type Container = Table;

    type CreateInfo = CreateColumnInfo;

    fn catalog_set(table: &Self::Container) -> &CatalogSet<Self> {
        &table.columns
    }

    fn new(_tx: &Transaction, info: Self::CreateInfo) -> Self {
        Self { name: info.name, ty: info.ty }
    }
}
