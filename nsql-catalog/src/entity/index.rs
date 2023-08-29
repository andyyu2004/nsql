use nsql_storage::eval::{Expr, TupleExpr};
use nsql_storage::IndexStorageInfo;

use super::*;
use crate::{Column, ColumnIdentity, ColumnIndex, Namespace};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple)]
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
    fn from_value(value: Value) -> Result<Self, CastError> {
        let kind = value.cast::<u8>()?;
        assert!(kind < IndexKind::__Last as u8);
        Ok(unsafe { std::mem::transmute(kind) })
    }
}

impl IntoTuple for Index {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::from([
            Value::Oid(self.table.untyped()),
            Value::Oid(self.target.untyped()),
            Value::Int64(self.kind as i64),
            self.index_expr.into(),
        ])
    }
}

impl SystemEntity for Index {
    type Parent = Namespace;

    type Key = <Table as SystemEntity>::Key;

    type SearchKey = !;

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        todo!()
    }

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
        Ok(catalog.get::<Table>(tx, self.table)?.name())
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<Table>(tx, self.table)?.parent_oid(catalog, tx)
    }

    fn bootstrap_column_info() -> Vec<Column> {
        let table = Self::table();

        vec![
            Column {
                table,
                index: ColumnIndex::new(0),
                ty: LogicalType::Oid,
                name: "table".into(),
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
            Column {
                table,
                index: ColumnIndex::new(1),
                ty: LogicalType::Oid,
                name: "target".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
            Column {
                table,
                index: ColumnIndex::new(2),
                ty: LogicalType::Int64,
                name: "kind".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
            Column {
                table,
                index: ColumnIndex::new(3),
                ty: LogicalType::TupleExpr,
                name: "index_expr".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
        ]
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::INDEX
    }
}
