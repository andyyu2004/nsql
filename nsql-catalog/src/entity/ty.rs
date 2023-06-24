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
        // can't use match because of structuraleq sadness
        if oid == Type::OID {
            LogicalType::Oid
        } else if oid == Type::BOOL {
            LogicalType::Bool
        } else if oid == Type::INT {
            LogicalType::Int
        } else if oid == Type::TEXT {
            LogicalType::Text
        } else if oid == Type::BYTEA {
            LogicalType::Bytea
        } else {
            panic!()
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
