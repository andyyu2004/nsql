use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Namespace {
    pub(crate) oid: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Namespace {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { oid: crate::hack_new_oid_tmp(), name }
    }

    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }
}

impl SystemEntity for Namespace {
    type Parent = ();

    type Key = Oid<Self>;

    type SearchKey = Name;

    #[inline]
    fn search_key(&self) -> Self::SearchKey {
        self.name()
    }

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
        "namespace"
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            Table::NAMESPACE.untyped(),
            vec![
                ColumnStorageInfo::new("oid", LogicalType::Oid, true),
                ColumnStorageInfo::new("name", LogicalType::Text, false),
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

    #[inline]
    fn table() -> Oid<Table> {
        Table::NAMESPACE
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}
