use nsql_storage::eval::Expr;

use super::*;
use crate::{Column, ColumnIdentity, ColumnIndex};

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

    fn bootstrap_column_info() -> Vec<Column> {
        let table = Self::table();

        vec![
            Column {
                table,
                index: ColumnIndex::new(0),
                ty: LogicalType::Oid,
                name: "oid".into(),
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
            Column {
                table,
                index: ColumnIndex::new(1),
                ty: LogicalType::Text,
                name: "name".into(),
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
            },
        ]
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(None)
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::NAMESPACE
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}
