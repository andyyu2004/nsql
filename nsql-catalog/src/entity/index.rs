use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Index {
    pub(crate) table: Oid<Table>,
    pub(crate) name: Name,
    pub(crate) kind: IndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKind {
    // PrimaryKey,
    // Unique,
    // NonUnique,
}

impl FromTuple for Index {
    fn from_tuple(_tuple: Tuple) -> Result<Self, FromTupleError> {
        todo!()
    }
}

impl IntoTuple for Index {
    fn into_tuple(self) -> Tuple {
        todo!()
    }
}

impl SystemEntity for Index {
    type Parent = ();

    type Id = <Table as SystemEntity>::Id;

    #[inline]
    fn id(&self) -> Self::Id {
        self.table
    }

    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "index"
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    #[inline]
    fn storage_info() -> TableStorageInfo {
        todo!()
    }
}
