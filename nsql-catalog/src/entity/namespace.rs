use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace {
    pub(crate) oid: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Namespace {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { oid: crate::hack_new_oid_tmp(), name }
    }
}

impl SystemEntity for Namespace {
    type Parent = ();

    type Id = Oid<Self>;

    #[inline]
    fn id(&self) -> Oid<Self> {
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
        "namespace"
    }

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::NAMESPACE.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        Ok(None)
    }
}

impl FromTuple for Namespace {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        Ok(Self {
            oid: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
            name: values.next().ok_or(FromTupleError::NotEnoughValues)?.cast_non_null()?,
        })
    }
}

impl IntoTuple for Namespace {
    fn into_tuple(self) -> Tuple {
        Tuple::from([Value::Oid(self.oid.untyped()), Value::Text(self.name.into())])
    }
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}
