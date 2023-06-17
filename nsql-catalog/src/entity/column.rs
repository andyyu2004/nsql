use super::*;
use crate::Type;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    pub(crate) table: Oid<Table>,
    pub(crate) index: ColumnIndex,
    pub(crate) ty: Oid<Type>,
    pub(crate) name: Name,
    pub(crate) is_primary_key: bool,
}

impl From<&Column> for ColumnStorageInfo {
    fn from(val: &Column) -> Self {
        ColumnStorageInfo::new(Type::oid_to_logical_type(val.ty), val.is_primary_key)
    }
}

impl Column {
    pub fn new(
        table: Oid<Table>,
        name: Name,
        index: ColumnIndex,
        ty: Oid<Type>,
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
        Type::oid_to_logical_type(self.ty)
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

impl FromValue for ColumnIndex {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        let index = value.cast_non_null::<u8>().map_err(CastError::cast)?;
        Ok(Self { index })
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

#[derive(Debug, Clone)]
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

    #[inline]
    fn key(&self) -> Self::Key {
        (self.table, self.index)
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
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Int, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
                ColumnStorageInfo::new(LogicalType::Bool, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::ATTRIBUTE
    }
}

impl FromTuple for Column {
    fn from_values(values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        let mut values = values;
        Ok(Self {
            table: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            index: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            ty: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            name: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            is_primary_key: values
                .next()
                .ok_or(FromTupleError::NotEnoughValues)?
                .cast_non_null()?,
        })
    }
}

impl IntoTuple for Column {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.table.untyped()),
            Value::Int32(self.index.index as i32),
            Value::Oid(self.ty.untyped()),
            Value::Text(self.name.into()),
            Value::Bool(self.is_primary_key),
        ])
    }
}
