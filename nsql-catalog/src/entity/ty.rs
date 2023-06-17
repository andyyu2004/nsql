use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Type {
    pub(crate) oid: Oid<Type>,
    pub(crate) name: Name,
}

impl SystemEntity for Type {
    type Parent = ();

    type Key = Oid<Self>;

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
        "type"
    }

    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(None)
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::TYPE.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::TYPE
    }
}

impl FromTuple for Type {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(Self {
            oid: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            name: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
        })
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
