#![deny(rust_2018_idioms)]
#![feature(never_type, once_cell_try, min_specialization, type_alias_impl_trait, lazy_cell)]

mod bootstrap;
mod entity;
pub mod expr;
mod system_table;

use std::fmt;
use std::hash::{BuildHasherDefault, Hash};

pub use anyhow::Error;
use dashmap::DashMap;
use expr::ExecutableFunction;
use memo_map::MemoMap;
use nsql_core::{Name, Oid};
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage::value::Value;
use nsql_storage_engine::{ExecutionMode, ReadWriteExecutionMode, StorageEngine};
use rustc_hash::FxHasher;

use self::bootstrap::{BootstrapColumn, BootstrapSequence};
pub use self::entity::column::{Column, ColumnIdentity, ColumnIndex};
pub use self::entity::function::{
    AggregateFunctionInstance, Function, FunctionKind, ScalarFunctionPtr,
};
pub use self::entity::index::{Index, IndexKind};
pub use self::entity::namespace::Namespace;
pub use self::entity::operator::{Operator, OperatorKind};
pub use self::entity::sequence::{Sequence, SequenceData};
pub use self::entity::table::{PrimaryKeyConflict, Table, TableStorage};
use self::private::SystemEntityPrivate;
pub use self::system_table::SystemTableView;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait TransactionContext<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    fn transaction(&self) -> &'txn M::Transaction;

    fn catalog_caches(&self) -> &TransactionLocalCatalogCaches<'env, 'txn, S, M>;
}

/// A cache containing all the system tables used within the scope of a transaction.
/// It is important to cache these as opening the table is expensive.
pub struct TransactionLocalCatalogCaches<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    table_columns: DashMap<Oid<Table>, Box<[Column]>, BuildHasherDefault<FxHasher>>,
    table_storages:
        MemoMap<Oid<Table>, TableStorage<'env, 'txn, S, M>, BuildHasherDefault<FxHasher>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Default
    for TransactionLocalCatalogCaches<'env, 'txn, S, M>
{
    #[inline]
    fn default() -> Self {
        Self { table_columns: Default::default(), table_storages: Default::default() }
    }
}

mod private {

    use super::*;
    use crate::entity::table::TableStorageInfo;
    pub trait SystemEntityPrivate {
        const TABLE: Oid<Table>;

        /// Returns the storage info for the table that is used to build the table during catalog bootstrap.
        fn bootstrap_column_info() -> Vec<BootstrapColumn>;

        fn bootstrap_table_storage_info() -> TableStorageInfo {
            TableStorageInfo::new(
                Self::TABLE,
                Self::bootstrap_column_info().into_iter().map(|c| c.into()).collect(),
            )
        }
    }
}

pub trait SystemEntity:
    SystemEntityPrivate + FromTuple + IntoTuple + Eq + Clone + fmt::Debug
{
    type Parent: SystemEntity;

    type Key: FromTuple + Eq + Hash + Copy + fmt::Debug;

    type SearchKey: Eq + Hash + fmt::Debug;

    fn key(&self) -> Self::Key;

    /// A unique key that can be used to search for this entity within it's parent.
    /// e.g. `name`
    fn search_key(&self) -> Self::SearchKey;

    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name, Error>;

    fn desc() -> &'static str;

    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>>;
}

impl SystemEntity for () {
    type Parent = ();

    type Key = ();

    type SearchKey = ();

    fn name<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Name> {
        unreachable!()
    }

    fn key(&self) -> Self::Key {}

    fn search_key(&self) -> Self::SearchKey {}

    fn desc() -> &'static str {
        "catalog"
    }

    fn parent_oid<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Option<Oid<Self::Parent>>> {
        unreachable!()
    }
}

impl SystemEntityPrivate for () {
    fn bootstrap_column_info() -> Vec<BootstrapColumn> {
        todo!()
    }

    const TABLE: Oid<Table> = panic!();
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
    pub fn get<'txn, M: ExecutionMode<'env, S>, T: SystemEntity>(
        self,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        oid: T::Key,
    ) -> Result<T>
    where
        'env: 'txn,
    {
        self.system_table(tx)?.get(oid)
    }

    #[inline]
    pub fn table<'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        oid: Oid<Table>,
    ) -> Result<Table>
    where
        'env: 'txn,
    {
        self.get::<M, Table>(tx, oid)
    }

    #[inline]
    pub fn namespaces<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Namespace>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn tables<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Table>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn indexes<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Index>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn functions<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Function>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn operators<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Operator>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn sequences<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Sequence>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn columns<'a, 'txn, M: ExecutionMode<'env, S>>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, Column>, S::Error>
    where
        'env: 'txn,
    {
        self.system_table(tx)
    }

    #[inline]
    #[track_caller]
    pub fn system_table<'a, 'txn, M: ExecutionMode<'env, S>, T: SystemEntity>(
        self,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<SystemTableView<'a, 'env, 'txn, S, M, T>, S::Error> {
        // We still need to open the table in bootstrap mode to avoid loading cyclic dependencies.
        // We currently only use indexes to check uniqueness not for lookups, so this isn't an issue yet.
        SystemTableView::new_bootstrap(self.storage(), tx)
    }

    pub fn drop_table<'txn>(
        &self,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        oid: Oid<Table>,
    ) -> Result<()>
    where
        'env: 'txn,
    {
        // FIXME assert we can't drop system tables, maybe reserve a range for bootstrap oids or something

        // delete entry in `nsql_catalog.nsql_table`
        // FIXME need to cleanup columns and any referencing data
        assert!(
            self.system_table::<ReadWriteExecutionMode, Table>(tx)?
                .delete(Value::Oid(oid.untyped()))?,
            "attempted to drop non-existent table, this should fail earlier"
        );

        // drop physical table in storage engine
        self.storage.drop_tree(tx.transaction(), &oid.to_string())?;

        Ok(())
    }
}

pub const MAIN_SCHEMA_PATH: &str = "main";

impl<'env, S: StorageEngine> Catalog<'env, S> {
    #[inline]
    pub fn new(storage: &'env S) -> Self {
        Self { storage }
    }

    #[inline]
    /// Create a blank catalog with the default schema
    pub fn create<'txn>(
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> Result<Self>
    where
        'env: 'txn,
    {
        bootstrap::bootstrap(storage, tx)?;

        let catalog = Self { storage };
        Ok(catalog)
    }

    #[inline]
    pub fn storage(&self) -> &'env S {
        self.storage
    }
}

pub trait FunctionCatalog<'env, 'txn, S, M, F = ExecutableFunction<'env, 'txn, S, M>> {
    fn storage(&self) -> &'env S;

    fn get_function(
        &self,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        oid: Oid<Function>,
    ) -> Result<F>
    where
        'env: 'txn;
}
