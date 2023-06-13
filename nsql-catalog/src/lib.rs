#![deny(rust_2018_idioms)]

mod bootstrap;
mod entity;
pub mod schema;
mod system_table;

use std::fmt;
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
pub use self::entity::namespace::{CreateNamespaceInfo, Namespace};
pub use self::entity::table::{CreateTableInfo, Table};
pub use self::entity::ty::Type;
use self::system_table::SystemTableView;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait SystemEntity: FromTuple + IntoTuple + Eq + fmt::Debug {
    type Parent: SystemEntity;

    fn oid(&self) -> Oid<Self>;

    fn name(&self) -> Name;

    fn desc() -> &'static str;

    fn parent_oid(&self) -> Option<Oid<Self::Parent>>;

    fn storage_info() -> TableStorageInfo;
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
    pub fn get<'txn, T: SystemEntity>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
        oid: Oid<T>,
    ) -> Result<T> {
        let table = SystemTableView::<S, ReadonlyExecutionMode, T>::new(self.storage, tx)?;
        table.get(oid)
    }

    #[inline]
    pub fn namespaces<'txn>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Namespace>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn tables<'txn>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Table>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn columns<'txn>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, Column>, S::Error> {
        self.system_table(tx)
    }

    #[inline]
    pub fn system_table<'txn, T: SystemEntity>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, T>, S::Error> {
        SystemTableView::new(self.storage, tx)
    }

    #[inline]
    pub fn system_table_write<'txn, T: SystemEntity>(
        &self,
        tx: &'txn S::WriteTransaction<'env>,
    ) -> Result<SystemTableView<'env, 'txn, S, ReadWriteExecutionMode, T>, S::Error> {
        SystemTableView::new(self.storage, tx)
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
