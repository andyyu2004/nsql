use core::fmt;
use std::mem;

use nsql_storage::expr::Expr;

use super::*;
use crate::{ColumnIdentity, Function, SystemEntityPrivate};

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Operator {
    pub(crate) oid: Oid<Self>,
    pub(crate) kind: OperatorKind,
    pub(crate) namespace: Oid<Namespace>,
    /// The function that implements this operator
    pub(crate) function: Oid<Function>,
    pub(crate) name: Name,
}

impl Operator {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    pub fn function(&self) -> Oid<Function> {
        self.function
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OperatorKind {
    Unary,
    Binary, // note: make sure the assertion below is changed if variants are reordered
}

impl FromValue for OperatorKind {
    fn from_value(value: Value) -> Result<Self, CastError> {
        let b = value.cast::<u8>()?;
        assert!(b <= OperatorKind::Binary as u8);
        Ok(unsafe { mem::transmute(b) })
    }
}

impl From<OperatorKind> for Value {
    #[inline]
    fn from(value: OperatorKind) -> Self {
        Value::Byte(value as u8)
    }
}

impl SystemEntity for Operator {
    type Parent = Namespace;

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        self.name()
    }

    #[inline]
    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        Ok(self.name())
    }

    #[inline]
    fn desc() -> &'static str {
        "operator"
    }

    #[inline]
    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }

    fn extract_cache<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        caches: &'a TransactionLocalCatalogCaches<'env, 'txn, S, M>,
    ) -> &'a OnceLock<SystemTableView<'env, 'txn, S, M, Self>> {
        &caches.operators
    }
}

impl SystemEntityPrivate for Operator {
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
                ty: LogicalType::Byte,
                name: "kind",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "namespace",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
            },
            BootstrapColumn {
                ty: LogicalType::Oid,
                name: "function",
                is_primary_key: false,
                identity: ColumnIdentity::None,
                default_expr: Expr::null(),
                seq: None,
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

    const TABLE: Oid<Table> = Table::OPERATOR;
}
