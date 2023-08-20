use core::fmt;
use std::mem;

use super::*;
use crate::Function;

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
    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        Ok(self.name())
    }

    #[inline]
    fn desc() -> &'static str {
        "operator"
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(Some(self.namespace))
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::OPERATOR.untyped(),
            vec![
                ColumnStorageInfo::new("oid", LogicalType::Oid, true),
                ColumnStorageInfo::new("kind", LogicalType::Byte, false),
                ColumnStorageInfo::new("namespace", LogicalType::Oid, true),
                ColumnStorageInfo::new("function", LogicalType::Oid, false),
                ColumnStorageInfo::new("name", LogicalType::Text, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::OPERATOR
    }
}
