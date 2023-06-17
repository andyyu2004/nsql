use nsql_storage::IndexStorageInfo;

use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Index {
    pub(crate) table: Oid<Table>,
    pub(crate) target: Oid<Table>,
    pub(crate) kind: IndexKind,
}

impl Index {
    pub fn storage_info<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<IndexStorageInfo> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IndexKind {
    Unique,
    NonUnique,
}

impl FromValue for IndexKind {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        let kind = value.cast_non_null::<u8>().map_err(CastError::cast)?;
        match kind {
            0 => Ok(Self::Unique),
            1 => Ok(Self::NonUnique),
            _ => panic!("invalid index kind: {}", kind),
        }
    }
}

impl FromTuple for Index {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(Self {
            table: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            target: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            kind: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
        })
    }
}

impl IntoTuple for Index {
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.table.untyped()),
            Value::Oid(self.target.untyped()),
            Value::Int32(self.kind as i32),
        ])
    }
}

impl SystemEntity for Index {
    type Parent = Namespace;

    type Id = <Table as SystemEntity>::Id;

    #[inline]
    fn id(&self) -> Self::Id {
        self.table
    }

    #[inline]
    fn desc() -> &'static str {
        "index"
    }

    #[inline]
    fn name<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        catalog.get::<Table>(tx, self.table)?.name(catalog, tx)
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<Table>(tx, self.table)?.parent_oid(catalog, tx)
    }

    #[inline]
    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::INDEX.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Oid, false),
                ColumnStorageInfo::new(LogicalType::Int, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::INDEX
    }
}
