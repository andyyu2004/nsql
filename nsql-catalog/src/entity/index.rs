use nsql_storage::eval::TupleExpr;
use nsql_storage::value::IntoValue;
use nsql_storage::IndexStorageInfo;

use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Index {
    pub(crate) table: Oid<Table>,
    pub(crate) target: Oid<Table>,
    pub(crate) kind: IndexKind,
    /// The expression to index on.
    /// This will usually be a projection of the target table's columns.
    pub(crate) index_expr: TupleExpr,
}

impl Index {
    #[inline]
    pub fn storage_info<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<IndexStorageInfo> {
        let table = catalog.get::<Table>(tx, self.table)?.table_storage_info(catalog, tx)?;
        Ok(IndexStorageInfo::new(table, self.index_expr.clone()))
    }

    #[inline]
    pub fn expr(&self) -> &TupleExpr {
        &self.index_expr
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IndexKind {
    Unique,
    __Last,
}

impl FromValue for IndexKind {
    #[inline]
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        let kind = value.cast_non_null::<u8>().map_err(CastError::cast)?;
        assert!(kind < IndexKind::__Last as u8);
        Ok(unsafe { std::mem::transmute(kind) })
    }
}

impl FromTuple for Index {
    #[inline]
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(Self {
            table: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            target: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            kind: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            index_expr: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
        })
    }
}

impl IntoTuple for Index {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.table.untyped()),
            Value::Oid(self.target.untyped()),
            Value::Int32(self.kind as i32),
            self.index_expr.into_value(),
        ])
    }
}

impl SystemEntity for Index {
    type Parent = Namespace;

    type Key = <Table as SystemEntity>::Key;

    #[inline]
    fn key(&self) -> Self::Key {
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
                ColumnStorageInfo::new(LogicalType::Bytea, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::INDEX
    }
}
