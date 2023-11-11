use nsql_storage::expr::TupleExpr;

use super::table::IndexStorageInfo;
use super::*;
use crate::{Namespace, SystemEntityPrivate, TransactionContext};

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
    pub fn storage_info<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<IndexStorageInfo> {
        let table = catalog.get::<M, Table>(tx, self.table)?.table_storage_info(catalog, tx)?;
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
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(catalog.get::<M, Table>(tx, self.table)?.name())
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<M, Table>(tx, self.table)?.parent_oid(catalog, tx)
    }
    fn extract_cache<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        caches: &'a TransactionLocalCatalogCaches<'env, 'txn, S, M>,
    ) -> &'a OnceLock<SystemTableView<'env, 'txn, S, M, Self>> {
        &caches.indexes
    }
}

impl SystemEntityPrivate for Index {
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "table",
                is_primary_key: true,
                ..Default::default()
            },
            BootstrapColumn { ty: LogicalType::Oid, name: "target", ..Default::default() },
            BootstrapColumn { ty: LogicalType::Int64, name: "kind", ..Default::default() },
            BootstrapColumn {
                ty: LogicalType::TupleExpr,
                name: "index_expr",
                ..Default::default()
            },
        ]
    }

    const TABLE: Oid<Table> = Table::INDEX;
}
