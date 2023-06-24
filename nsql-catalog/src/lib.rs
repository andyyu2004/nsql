#![deny(rust_2018_idioms)]
#![feature(never_type)]

mod bootstrap;
mod entity;
pub mod schema;
mod system_table;

use std::fmt;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;

pub use anyhow::Error;
use nsql_core::{Name, Oid};
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage::value::Value;
use nsql_storage::TableStorageInfo;
use nsql_storage_engine::{
    ReadWriteExecutionMode, ReadonlyExecutionMode, StorageEngine, Transaction,
};

pub use self::entity::column::{Column, ColumnIndex, CreateColumnInfo};
pub use self::entity::function::Function;
pub use self::entity::index::{Index, IndexKind};
pub use self::entity::namespace::{CreateNamespaceInfo, Namespace};
pub use self::entity::table::{CreateTableInfo, Table};
use self::system_table::SystemTableView;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait SystemEntity: FromTuple + IntoTuple + Eq + fmt::Debug {
    type Parent: SystemEntity;

    type Key: FromTuple + Eq + Hash + fmt::Debug;

    type SearchKey: Eq + Hash + fmt::Debug;

    fn key(&self) -> Self::Key;

    /// A unique key that can be used to search for this entity within it's parent.
    /// e.g. `name`
    fn search_key(&self) -> Self::SearchKey;

    fn name<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Name, Error>;

    fn desc() -> &'static str;

    fn parent_oid<'env, S: StorageEngine>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>>;

    fn table() -> Oid<Table>;

    /// Returns the storage info for the table that is used to build the table during catalog bootstrap.
    fn bootstrap_table_storage_info() -> TableStorageInfo;
}

impl SystemEntity for () {
    type Parent = ();

    type Key = ();

    type SearchKey = ();

    fn name<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Name> {
        unreachable!()
    }

    fn key(&self) -> Self::Key {}

    fn search_key(&self) -> Self::SearchKey {}

    fn desc() -> &'static str {
        "catalog"
    }

    fn bootstrap_table_storage_info() -> TableStorageInfo {
        todo!()
    }

    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        unreachable!()
    }

    fn table() -> Oid<Table> {
        todo!()
    }
}

pub struct Catalog<'env, S> {
    storage: &'env S,
}

impl<'env, S> Clone for Catalog<'env, S> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

fn hack_new_oid_tmp<T>() -> Oid<T> {
    static NEXT: AtomicU64 = AtomicU64::new(1000);
    Oid::new(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

impl<'env, S> Copy for Catalog<'env, S> {}

impl<'env, S: StorageEngine> Catalog<'env, S> {
    #[inline]
    pub fn get<'txn, T: SystemEntity>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
        oid: T::Key,
    ) -> Result<T> {
        // must open in bootstrap to avoid cycles for now
        SystemTableView::<S, ReadonlyExecutionMode, T>::new_bootstrap(self.storage(), tx)?.get(oid)
    }

    #[inline]
    pub fn table<'txn>(self, tx: &'txn dyn Transaction<'env, S>, oid: Oid<Table>) -> Result<Table> {
        self.get(tx, oid)
    }

    #[inline]
    pub fn namespaces<'txn>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Namespace>> {
        self.system_table(tx)
    }

    #[inline]
    pub fn tables<'txn>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Table>> {
        self.system_table(tx)
    }

    #[inline]
    pub fn columns<'txn>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Column>> {
        self.system_table(tx)
    }

    #[inline]
    pub fn system_table<'txn, T: SystemEntity>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, T>> {
        // When opening in read-only mode, we still open the table in bootstrap mode to avoid loading cyclic dependencies.
        // We currently only use indexes to check uniqueness not for lookups, so this isn't an issue yet.
        Ok(SystemTableView::new_bootstrap(self.storage(), tx)?)
    }

    #[inline]
    pub fn system_table_write<'txn, T: SystemEntity>(
        self,
        tx: &'txn S::WriteTransaction<'env>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadWriteExecutionMode, T>> {
        SystemTableView::new(self, tx)
    }

    pub fn drop_table(&self, tx: &S::WriteTransaction<'env>, oid: Oid<Table>) -> Result<()> {
        // FIXME assert we can't drop system tables, maybe reserve a range for bootstrap oids or something

        // delete entry in `nsql_catalog.nsql_table`
        assert!(
            self.system_table_write::<Table>(tx)?.delete(Value::Oid(oid.untyped()))?,
            "attempted to drop non-existent table, this should fail earlier"
        );

        // drop physical table in storage engine
        self.storage.drop_tree(tx, &oid.to_string())?;

        Ok(())
    }
}

pub const MAIN_SCHEMA: &str = "main";

impl<'env, S: StorageEngine> Catalog<'env, S> {
    pub fn open(storage: &'env S) -> Self {
        Self { storage }
    }

    /// Create a blank catalog with the default schema
    pub fn create(storage: &'env S, tx: &S::WriteTransaction<'env>) -> Result<Self> {
        bootstrap::bootstrap(storage, tx)?;

        let catalog = Self { storage };
        Ok(catalog)
    }

    pub(crate) fn storage(&self) -> &'env S {
        self.storage
    }
}
