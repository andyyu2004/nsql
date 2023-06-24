use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Type {
    pub(crate) oid: Oid<Type>,
    pub(crate) name: Name,
}

impl Type {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }
}

impl SystemEntity for Type {
    type Parent = ();

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn key(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn search_key(&self) -> Name {
        self.name()
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

impl Type {
    pub fn oid_to_logical_type(oid: Oid<Self>) -> LogicalType {
        match oid {
            Type::OID => LogicalType::Oid,
            Type::BOOL => LogicalType::Bool,
            Type::INT => LogicalType::Int,
            Type::TEXT => LogicalType::Text,
            Type::BYTEA => LogicalType::Bytea,
            _ => panic!(),
        }
    }

    pub fn logical_type_to_oid(logical_type: &LogicalType) -> Oid<Self> {
        match logical_type {
            LogicalType::Oid => Type::OID,
            LogicalType::Bool => Type::BOOL,
            LogicalType::Int => Type::INT,
            LogicalType::Text => Type::TEXT,
            LogicalType::Bytea => Type::BYTEA,
            LogicalType::Null => todo!(),
            LogicalType::Decimal => todo!(),
            LogicalType::Array(_) => todo!(),
        }
    }
}
