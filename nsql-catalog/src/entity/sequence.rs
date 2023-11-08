use nsql_storage::expr::Expr;

use super::*;
use crate::{ColumnIdentity, SystemEntityPrivate};

/// Model of a row that lives in `nsql_catalog.nsql_sequence`
#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Sequence {
    pub(crate) oid: Oid<Table>,
    pub(crate) start: i64,
    pub(crate) step: i64,
}

impl Sequence {
    pub fn new(oid: Oid<Table>) -> Self {
        Self { oid, start: 1, step: 1 }
    }

    #[inline]
    pub fn oid(&self) -> Oid<Table> {
        self.oid
    }
}

/// Model of the data that lives in the backing table of a particular sequence
// This needs to match the column definitions in bootstrap
#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct SequenceData {
    pub key: Oid<SequenceData>,
    pub(crate) value: i64,
}

impl SequenceData {
    // There is only one row in a sequence backing table, so we just use a constant
    pub(crate) const KEY: Oid<SequenceData> = Oid::new(42);

    #[inline]
    pub fn new(value: i64) -> Self {
        Self { key: Self::KEY, value }
    }
}

impl SystemEntity for Sequence {
    type Parent = Namespace;

    type Key = Oid<Table>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        todo!()
    }

    #[inline]
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(catalog.get::<M, Table>(tx, self.oid)?.name())
    }

    #[inline]
    fn desc() -> &'static str {
        "sequence"
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<M, Table>(tx, self.oid)?.parent_oid(catalog, tx)
    }
}

impl SystemEntityPrivate for Sequence {
    #[inline]
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        vec![
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "oid",
                is_primary_key: true,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Int64,
                name: "start",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::literal(1),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Int64,
                name: "step",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::literal(1),
                seq: None,
            },
        ]
    }

    const TABLE: Oid<Table> = Table::SEQUENCE;
}
