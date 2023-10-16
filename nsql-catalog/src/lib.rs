#![deny(rust_2018_idioms)]
#![feature(never_type)]

mod bootstrap;
mod entity;
mod system_table;

use std::fmt;
use std::hash::Hash;

pub use anyhow::Error;
use nsql_core::{Name, Oid};
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage::value::Value;
use nsql_storage::TableStorageInfo;
use nsql_storage_engine::{
    ReadWriteExecutionMode, ReadonlyExecutionMode, StorageEngine, Transaction,
};

use self::bootstrap::{BootstrapColumn, BootstrapSequence};
pub use self::entity::column::{Column, ColumnIdentity, ColumnIndex};
pub use self::entity::function::{
    AggregateFunctionInstance, Function, FunctionKind, ScalarFunction,
};
pub use self::entity::index::{Index, IndexKind};
pub use self::entity::namespace::Namespace;
pub use self::entity::operator::{Operator, OperatorKind};
pub use self::entity::sequence::{Sequence, SequenceData};
pub use self::entity::table::Table;
use self::private::SystemEntityPrivate;
pub use self::system_table::SystemTableView;

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod private {
    use super::*;
    pub trait SystemEntityPrivate {
        fn table() -> Oid<Table>;

        /// Returns the storage info for the table that is used to build the table during catalog bootstrap.
        fn bootstrap_column_info() -> Vec<BootstrapColumn>;

        fn bootstrap_table_storage_info() -> TableStorageInfo {
            TableStorageInfo::new(
                Self::table().untyped(),
                Self::bootstrap_column_info().into_iter().map(|c| c.into()).collect(),
            )
        }
    }
}

pub trait SystemEntity: SystemEntityPrivate + FromTuple + IntoTuple + Eq + fmt::Debug {
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

    fn parent_oid<'env, S: StorageEngine>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        unreachable!()
    }
}

impl SystemEntityPrivate for () {
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        todo!()
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
    pub fn functions<'txn>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Function>> {
        self.system_table(tx)
    }

    #[inline]
    pub fn operators<'txn>(
        self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Operator>> {
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
        // FIXME need to cleanup columns and any referencing data
        assert!(
            self.system_table_write::<Table>(tx)?.delete(Value::Oid(oid.untyped()))?,
            "attempted to drop non-existent table, this should fail earlier"
        );

        // drop physical table in storage engine
        self.storage.drop_tree(tx, &oid.to_string())?;

        Ok(())
    }
}

pub const MAIN_SCHEMA_PATH: &str = "main";

impl<'env, S: StorageEngine> Catalog<'env, S> {
    pub fn new(storage: &'env S) -> Self {
        Self { storage }
    }

    /// Create a blank catalog with the default schema
    pub fn create(storage: &'env S, tx: &S::WriteTransaction<'env>) -> Result<Self> {
        bootstrap::bootstrap(storage, tx)?;

        let catalog = Self { storage };
        Ok(catalog)
    }

    #[inline]
    pub fn storage(&self) -> &'env S {
        self.storage
    }
}
