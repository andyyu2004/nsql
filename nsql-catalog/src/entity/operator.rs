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
    /// The type of the lhs operand, `NULL` if prefix operator
    pub(crate) left: LogicalType,
    pub(crate) right: LogicalType,
    pub(crate) output: LogicalType,
}

impl Operator {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    pub fn left_ty(&self) -> LogicalType {
        self.left.clone()
    }

    #[inline]
    pub fn right_ty(&self) -> LogicalType {
        self.right.clone()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OperatorKind {
    Prefix,
    Infix, // note: make sure the assertion below is changed if variants are reordered
}

impl FromValue for OperatorKind {
    fn from_value(value: Value) -> Result<Self, CastError<Self>> {
        match value {
            Value::Byte(b) => {
                assert!(b <= OperatorKind::Infix as u8);
                Ok(unsafe { mem::transmute(b) })
            }
            _ => Err(CastError::new(value)),
        }
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

    type SearchKey = (Name, LogicalType, LogicalType);

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        (self.name(), self.left_ty(), self.right_ty())
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
                ColumnStorageInfo::new("left", LogicalType::Type, true),
                ColumnStorageInfo::new("right", LogicalType::Type, true),
                ColumnStorageInfo::new("ret", LogicalType::Type, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::OPERATOR
    }
}
