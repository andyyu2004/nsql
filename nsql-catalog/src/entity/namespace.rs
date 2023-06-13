// use nsql_storage_engine::StorageEngine;

// use crate::private::CatalogEntity;
// use crate::set::CatalogSet;
// use crate::{Catalog, Container, Entity, Name, Oid, Table};

// #[derive(Debug)]
// pub struct Namespace<S> {
//     oid: Oid<Self>,
//     name: Name,
//     pub(crate) tables: CatalogSet<S, Table<S>>,
// }
//

use nsql_core::{LogicalType, Name, Oid};
use nsql_storage::tuple::{FromTuple, FromTupleError, IntoTuple, Tuple};
use nsql_storage::value::Value;
use nsql_storage::{ColumnStorageInfo, TableStorageInfo};

use crate::{bootstrap, SystemEntity};

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
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn oid(&self) -> Oid<Self> {
        self.oid
    }

    #[inline]
    fn parent_oid(&self) -> Option<Oid<Self::Parent>> {
        None
    }

    fn desc() -> &'static str {
        "namespace"
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

// pub trait NamespaceEntity<S: StorageEngine>: CatalogEntity<S, Container = Namespace<S>> {}
//
// impl<S: StorageEngine, T: CatalogEntity<S, Container = Namespace<S>>> NamespaceEntity<S> for T {}
//
// impl<S: StorageEngine> Container<S> for Namespace<S> {}

#[derive(Debug, Clone)]
pub struct CreateNamespaceInfo {
    pub name: Name,
    pub if_not_exists: bool,
}
//
// impl<S: StorageEngine> CatalogEntity<S> for Namespace<S> {
//     type Container = Catalog;
//
//     type CreateInfo = CreateNamespaceInfo;
//
//     #[inline]
//     fn catalog_set(catalog: Catalog<'_, S>,) -> Catalog<'_, S>,Set<S, Self> {
//         Catalog<'_, S>,.schemas
//     }
//
//     #[inline]
//     fn create(
//         _tx: &S::WriteTransaction<'_>,
//         _container: &Self::Container,
//         oid: Oid<Self>,
//         info: Self::CreateInfo,
//     ) -> Self {
//         Self { oid, name: info.name, tables: Default::default() }
//     }
// }
//
// impl<S: StorageEngine> Entity for Namespace<S> {
//     #[inline]
//     fn oid(&self) -> Oid<Self> {
//         self.oid
//     }
//
//     #[inline]
//     fn name(&self) -> Name {
//         Name::clone(&self.name)
//     }
//
//     #[inline]
//     fn desc() -> &'static str {
//         // we still call this a "schema" in the sql world, but not internally to avoid confusion
//         // with the other schema
//         "schema"
//     }
// }
