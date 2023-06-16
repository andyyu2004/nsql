use super::*;
use crate::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Index {
    pub(crate) table: Table,
    pub(crate) kind: IndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKind {
    PrimaryKey,
    // Unique,
    // NonUnique,
}

impl FromTuple for Index {
    fn from_values(mut values: impl Iterator<Item = Value>) -> Result<Self, FromTupleError> {
        let table = Table::from_values(&mut values)?;
        let kind = IndexKind::PrimaryKey;
        Ok(Self { table, kind })
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
        self.table.id()
    }

    #[inline]
    fn name(&self) -> Name {
        self.table.name()
    }

    #[inline]
    fn desc() -> &'static str {
        "index"
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        self.table.parent_oid()
    }

    #[inline]
    fn storage_info() -> TableStorageInfo {
        todo!()
    }
}
