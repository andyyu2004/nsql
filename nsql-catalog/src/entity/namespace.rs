use super::*;
use crate::bootstrap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace {
    pub(crate) oid: Oid<Namespace>,
    pub(crate) name: Name,
}

impl Namespace {
    pub const MAIN: Oid<Self> = Oid::new(101);

    pub(crate) const CATALOG: Oid<Self> = Oid::new(100);
}

impl Namespace {
    #[inline]
    pub fn new(name: Name) -> Self {
        Self { oid: crate::hack_new_oid_tmp(), name }
    }
}

impl Entity for Namespace {
    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn name(&self) -> Name {
        Name::clone(&self.name)
    }

    #[inline]
    fn desc() -> &'static str {
        "namespace"
    }
}

impl SystemEntity for Namespace {
    type Parent = ();

    fn storage_info() -> TableStorageInfo {
        TableStorageInfo::new(
            bootstrap::oid::TABLE_NAMESPACE.untyped(),
            vec![
                ColumnStorageInfo::new(LogicalType::Oid, true),
                ColumnStorageInfo::new(LogicalType::Text, false),
            ],
        )
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }
}

impl FromTuple for Namespace {
    fn from_tuple(mut tuple: Tuple) -> Result<Self, FromTupleError> {
        if tuple.len() != 2 {
            return Err(FromTupleError::ColumnCountMismatch { expected: 2, actual: tuple.len() });
        }

        Ok(Self { oid: tuple[0].take().cast_non_null()?, name: tuple[1].take().cast_non_null()? })
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
