use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl FromTuple for Function {
    fn from_values(values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        todo!()
    }
}

impl IntoTuple for Function {
    #[inline]
    fn into_tuple(self) -> Tuple {
        Tuple::from([])
    }
}
