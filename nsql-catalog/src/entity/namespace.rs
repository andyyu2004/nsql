use nsql_storage::expr::Expr;

use super::*;
use crate::{ColumnIdentity, Function, SystemEntityPrivate};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Namespace {
    pub(crate) oid: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Namespace {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    pub fn oid(&self) -> Oid<Namespace> {
        self.oid
    }
}

impl SystemEntity for Namespace {
    type Parent = ();

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        self.name()
    }

    #[inline]
    fn key(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    #[inline]
    fn desc() -> &'static str {
        "namespace"
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(None)
    }

    #[inline]
    fn extract_cache<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        caches: &'a TransactionLocalCatalogCaches<'env, 'txn, S, M>,
    ) -> &'a OnceLock<SystemTableView<'env, 'txn, S, M, Self>> {
        &caches.namespaces
    }
}

impl SystemEntityPrivate for Namespace {
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "oid",
                is_primary_key: true,
                identity: ColumnIdentity::Always,
                default_expr: Expr::call(
                    Function::NEXTVAL_OID.untyped(),
                    [Value::Oid(Table::NAMESPACE_OID_SEQ.untyped())],
                ),
                seq: Some(BootstrapSequence {
                    table: Table::NAMESPACE_OID_SEQ,
                    name: "nsql_namespace_oid_seq",
                }),
            },
            BootstrapColumn {
                ty: LogicalType::Text,
                name: "name",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
        ]
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::NAMESPACE
    }
}
