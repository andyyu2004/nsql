use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Index {
    pub(crate) table: Oid<Table>,
    pub(crate) kind: IndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKind {
    PrimaryKey,
    // Unique,
    // NonUnique,
}

impl FromTuple for Index {
    fn from_values(_values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        todo!()
        // let kind = IndexKind::PrimaryKey;
        // Ok(Self { table, kind })
    }
}

impl IntoTuple for Index {
    fn into_tuple(self) -> Tuple {
        todo!()
    }
}

impl SystemEntity for Index {
    type Parent = Namespace;

    type Id = <Table as SystemEntity>::Id;

    #[inline]
    fn id(&self) -> Self::Id {
        self.table
    }

    #[inline]
    fn desc() -> &'static str {
        "index"
    }

    #[inline]
    fn name<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        catalog.get::<Table>(tx, self.table)?.name(catalog, tx)
    }

    #[inline]
    fn parent_oid<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        catalog.get::<Table>(tx, self.table)?.parent_oid(catalog, tx)
    }

    #[inline]
    fn storage_info() -> TableStorageInfo {
        todo!()
    }
}
