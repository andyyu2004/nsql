use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_core::Oid;
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage::{TableStorage, TableStorageInfo};
use nsql_storage_engine::{ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine};

use crate::bootstrap::CatalogPath;
use crate::Result;

pub trait SystemEntity: FromTuple + IntoTuple + Eq + fmt::Debug {
    type Parent: SystemEntity;

    fn oid(&self) -> Oid<Self>;

    fn name(&self) -> &str;

    fn parent_oid(&self) -> Option<Oid<Self::Parent>>;

    fn storage_info() -> TableStorageInfo;

    fn desc() -> &'static str;

    #[inline]
    fn path(&self) -> CatalogPath<Self> {
        CatalogPath::new(self.oid(), self.parent_oid())
    }
}

#[repr(transparent)]
pub struct SystemTableView<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T> {
    storage: TableStorage<'env, 'txn, S, M>,
    phantom: PhantomData<T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    pub fn new(storage: &'env S, tx: M::TransactionRef<'txn>) -> Result<Self, S::Error> {
        let storage = TableStorage::<'env, 'txn, S, M>::open(storage, tx, T::storage_info())?;

        Ok(Self { storage, phantom: PhantomData })
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    #[inline]
    pub fn get(&self, oid: Oid<T>) -> Result<T> {
        Ok(self.scan()?.find(|entry| Ok(entry.oid() == oid))?.expect("got invalid oid"))
    }

    #[inline]
    pub fn find(&self, parent: Option<Oid<T::Parent>>, name: &str) -> Result<Option<T>> {
        self.scan()?.find(|entry| Ok(entry.parent_oid() == parent && entry.name() == name))
    }

    #[inline]
    #[fix_hidden_lifetime_bug]
    pub fn scan(&self) -> Result<impl FallibleIterator<Item = T, Error = anyhow::Error> + '_> {
        Ok(self.storage.scan(None)?.map_err(Into::into).map(|tuple| Ok(T::from_tuple(tuple)?)))
    }

    #[inline]
    #[fix_hidden_lifetime_bug]
    pub fn scan_arc(
        self: Arc<Self>,
    ) -> Result<impl FallibleIterator<Item = T, Error = anyhow::Error> + 'txn> {
        // Safety
        // We are effectively transmuting the type within the `Arc`
        // This is safe because we are ``#[repr(transparent)]``
        let raw = Arc::<Self>::into_raw(self);
        let inner = unsafe { Arc::<TableStorage<'env, 'txn, S, M>>::from_raw(raw as _) };
        Ok(inner.scan_arc(None)?.map_err(Into::into).map(|tuple| Ok(T::from_tuple(tuple)?)))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T: SystemEntity>
    SystemTableView<'env, 'txn, S, ReadWriteExecutionMode, T>
{
    #[inline]
    pub fn insert(&mut self, value: T) -> Result<()> {
        self.storage.insert(&value.into_tuple())
    }
}
