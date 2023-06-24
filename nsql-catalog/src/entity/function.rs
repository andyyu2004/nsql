use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash, FromTuple, IntoTuple)]
pub struct Function {
    pub(crate) oid: Oid<Self>,
    pub(crate) namespace: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Function {
    #[inline]
    pub fn name(&self) -> Name {
        Name::clone(&self.name)
    }
}

impl SystemEntity for Function {
    type Parent = Namespace;

    type Key = Oid<Self>;

    #[inline]
    fn key(&self) -> Self::Key {
        self.oid
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
        "function"
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
        TableStorageInfo::new(Table::ATTRIBUTE.untyped(), vec![])
    }

    #[inline]
    fn table() -> Oid<Table> {
        Table::ATTRIBUTE
    }
}
