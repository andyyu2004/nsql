use std::fmt;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Column {
    pub(crate) table: Oid<Table>,
    pub(crate) index: ColumnIndex,
    pub(crate) ty: LogicalType,
    pub(crate) name: Name,
    pub(crate) is_primary_key: bool,
}

impl From<&Column> for ColumnStorageInfo {
    fn from(col: &Column) -> Self {
        ColumnStorageInfo::new(col.name.clone(), col.ty.clone(), col.is_primary_key)
    }
}

impl Column {
    pub fn new(
        table: Oid<Table>,
        name: Name,
        index: ColumnIndex,
        ty: LogicalType,
        is_primary_key: bool,
    ) -> Self {
        Self { table, name, index, ty, is_primary_key }
    }

    #[inline]
    pub fn index(&self) -> ColumnIndex {
        self.index
    }

    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    pub fn logical_type(&self) -> LogicalType {
        self.ty.clone()
    }

    #[inline]
    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnIndex {
    index: u8,
}

impl fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.index)
    }
}

impl FromValue for ColumnIndex {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        let index = value.cast::<u8>().map_err(CastError::cast)?;
        Ok(Self { index })
    }
}

impl From<ColumnIndex> for Value {
    #[inline]
    fn from(val: ColumnIndex) -> Self {
        Value::Int64(val.index as i64)
    }
}

impl ColumnIndex {
    // FIXME ideally this would be private
    #[inline]
    pub fn new(index: u8) -> Self {
        Self { index }
    }

    #[inline]
    pub fn as_usize(self) -> usize {
        self.index as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateColumnInfo {
    pub name: Name,
    /// The index of the column in the table.
    pub index: u8,
    pub ty: LogicalType,
    pub is_primary_key: bool,
}

impl SystemEntity for Column {
    type Parent = Table;

    type Key = (Oid<Self::Parent>, ColumnIndex);

    type SearchKey = (Oid<Self::Parent>, Name);

    #[inline]
    fn key(&self) -> Self::Key {
        (self.table, self.index)
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        (self.table, self.name())
    }

    #[inline]
    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    #[inline]
    fn desc() -> &'static str {
        "column"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.table))
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::ATTRIBUTE.untyped(),
            vec![
                ColumnStorageInfo::new("table", LogicalType::Oid, true),
                ColumnStorageInfo::new("index", LogicalType::Int64, true),
                ColumnStorageInfo::new("ty", LogicalType::Type, false),
                ColumnStorageInfo::new("name", LogicalType::Text, false),
                ColumnStorageInfo::new("is_primary_key", LogicalType::Bool, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::ATTRIBUTE
    }
}
