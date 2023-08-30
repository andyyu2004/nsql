use nsql_storage::eval::Expr;

use super::*;
use crate::{ColumnIdentity, SystemEntityPrivate};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Sequence {
    pub(crate) table: Oid<Table>,
}

impl Sequence {
    pub fn new(table: Oid<Table>) -> Self {
        Self { table }
    }

    #[inline]
    pub fn oid(&self) -> Oid<Table> {
        self.table
    }
}

impl SystemEntity for Sequence {
    type Parent = Namespace;

    type Key = Oid<Table>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Self::Key {
        self.table
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        todo!()
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
    fn desc() -> &'static str {
        "sequence"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<Table>(tx, self.table)?.parent_oid(catalog, tx)
    }
}

impl SystemEntityPrivate for Sequence {
    #[inline]
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![BootstrapColumn {
            ty: LogicalType::Oid,
            name: "oid",
            is_primary_key: true,
            identity: ColumnIdentity::None,
            default_expr: Expr::null(),
            seq: None,
        }]
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::SEQUENCE
    }
}
