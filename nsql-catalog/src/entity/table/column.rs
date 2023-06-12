use nsql_core::LogicalType;
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::{CastError, FromValue, Value};
use nsql_storage::{ColumnStorageInfo, TableStorageInfo};

use crate::bootstrap::Type;
use crate::{Entity, Name, Oid, SystemEntity, Table};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    pub(crate) oid: Oid<Self>,
    pub(crate) table: Oid<Table>,
    pub(crate) name: Name,
    pub(crate) index: ColumnIndex,
    pub(crate) ty: Oid<Type>,
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
        Self { oid: crate::hack_new_oid_tmp(), table, name, index, ty, is_primary_key }
    }

    #[inline]
    pub fn index(&self) -> ColumnIndex {
        self.index
    }

    #[inline]
    pub fn logical_type(&self) -> LogicalType {
        todo!();
        // self.ty.clone()
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

impl Entity for Column {
    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "column"
    }
}

// impl CatalogEntity for Column {
//     type Container = Table<S>;
//
//     type CreateInfo = CreateColumnInfo;
//
//     fn catalog_set(table: &Self::Container) -> Catalog<'_, S>,Set<S, Self> {
//         &table.columns
//     }
//
//     fn create(
//         _tx: &S::WriteTransaction<'_>,
//         container: &Self::Container,
//         oid: Oid<Self>,
//         info: Self::CreateInfo,
//     ) -> Self {
//         Self {
//             oid,
//             table_oid: container.oid(),
//             name: info.name,
//             index: ColumnIndex::new(info.index),
//             ty: info.ty,
//             is_primary_key: info.is_primary_key,
//         }
//     }
// }

// #[derive(Debug)]
// pub struct ColumnRef {
//     pub table_ref: TableRef,
//     pub column: Oid<Column>,
// }
//
// impl<S> Clone for ColumnRef {
//     #[inline]
//     fn clone(&self) -> Self {
//         *self
//     }
// }
//
// impl<S> Copy for ColumnRef<S> {}

// impl<S: StorageEngine> EntityRef<S> for ColumnRef<S> {
//     type Entity = Column<S>;
//
//     type Container = Table<S>;
//
//     #[inline]
//     fn container(self, catalog: Catalog<'_, S>, tx: &dyn Transaction<'_, S>) -> Arc<Self::Container> {
//         self.table_ref.get(catalog, tx)
//     }
//
//     #[inline]
//     fn entity_oid(self) -> Oid<Self::Entity> {
//         self.column
//     }
// }

impl SystemEntity for Column {
    type Parent = Table;

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        Some(self.table)
    }

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            "nsql_catalog.nsql_column",
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Text, false),
                ColumnStorageInfo::new(LogicalType::Int, false),
                ColumnStorageInfo::new(LogicalType::Bool, false),
            ],
        )
    }

    fn desc() -> &'static str {
        "column"
    }
}

impl FromTuple for Column {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 6 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 6, actual: tuple.len() });
        }

        Ok(Self {
            oid: tuple[0].take().cast_non_null()?,
            table: tuple[1].take().cast_non_null()?,
            ty: tuple[2].take().cast_non_null()?,
            name: tuple[3].take().cast_non_null()?,
            index: tuple[4].take().cast_non_null()?,
            is_primary_key: tuple[5].take().cast_non_null()?,
        })
    }
}

impl IntoTuple for Column {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.oid.untyped()),
            Value::Oid(self.table.untyped()),
            Value::Oid(self.ty.untyped()),
            Value::Text(self.name.into()),
            Value::Int32(self.index.index as i32),
            Value::Bool(self.is_primary_key),
        ])
    }
}
