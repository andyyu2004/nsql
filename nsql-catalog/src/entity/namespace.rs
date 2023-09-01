use nsql_storage::eval::Expr;

use super::*;
use crate::{ColumnIdentity, Function, SystemEntityPrivate};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Namespace {
    pub(crate) oid: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Namespace {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { oid: crate::hack_new_oid_tmp(), name }
    }

    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
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
    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(Name::clone(&self.name))
    }

    #[inline]
    fn desc() -> &'static str {
        "namespace"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(None)
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

// FIXME remove
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}
