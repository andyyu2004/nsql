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

// pub trait NamespaceEntity<S: StorageEngine>: CatalogEntity<S, Container = Namespace<S>> {}
//
// impl<S: StorageEngine, T: CatalogEntity<S, Container = Namespace<S>>> NamespaceEntity<S> for T {}
//
// impl<S: StorageEngine> Container<S> for Namespace<S> {}
//
// #[derive(Debug)]
// pub struct CreateNamespaceInfo {
//     pub name: Name,
// }
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
