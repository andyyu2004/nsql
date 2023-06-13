use super::*;

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
            Table::TYPE.untyped(),
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
            Type::OID => LogicalType::Oid,
            Type::BOOL => LogicalType::Bool,
            Type::INT => LogicalType::Int,
            Type::TEXT => LogicalType::Text,
            _ => panic!(),
        }
    }

    pub fn logical_type_to_oid(logical_type: &LogicalType) -> Oid<Self> {
        match logical_type {
            LogicalType::Oid => Type::OID,
            LogicalType::Bool => Type::BOOL,
            LogicalType::Int => Type::INT,
            LogicalType::Text => Type::TEXT,
            _ => todo!(),
        }
    }
}
