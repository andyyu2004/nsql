use super::*;
use crate::bootstrap::oid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Type {
    pub(crate) oid: Oid<Type>,
    pub(crate) name: Name,
}

impl SystemEntity for Type {
    type Parent = ();

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }
    #[inline]
    fn desc() -> &'static str {
        "type"
    }

    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            oid::TABLE_TYPE.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }
}

impl FromTuple for Type {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(Type { oid: tuple[0].take().cast_non_null()?, name: tuple[1].take().cast_non_null()? })
    }
}

impl IntoTuple for Type {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name.into())])
    }
}

impl Type {
    pub fn oid_to_logical_type(oid: Oid<Self>) -> LogicalType {
        match oid {
            oid::TY_OID => LogicalType::Oid,
            oid::TY_BOOL => LogicalType::Bool,
            oid::TY_INT => LogicalType::Int,
            oid::TY_TEXT => LogicalType::Text,
            _ => panic!(),
        }
    }

    pub fn logical_type_to_oid(logical_type: &LogicalType) -> Oid<Self> {
        match logical_type {
            LogicalType::Oid => oid::TY_OID,
            LogicalType::Bool => oid::TY_BOOL,
            LogicalType::Int => oid::TY_INT,
            LogicalType::Text => oid::TY_TEXT,
            _ => todo!(),
        }
    }
}
