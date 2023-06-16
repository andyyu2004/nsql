use std::marker::PhantomData;
use std::sync::Arc;

use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_core::Oid;
use nsql_storage::tuple::IntoTuple;
use nsql_storage::TableStorage;
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

use crate::{Catalog, Result, SystemEntity};

#[repr(transparent)]
pub struct SystemTableView<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T> {
    storage: TableStorage<'env, 'txn, S, M>,
    phantom: PhantomData<T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    pub fn new(storage: &'env S, tx: M::TransactionRef<'txn>) -> Result<Self, S::Error> {
        // todo indexes
        let storage =
            TableStorage::<'env, 'txn, S, M>::open(storage, tx, T::table_storage_info(), todo!())?;

        Ok(Self { storage, phantom: PhantomData })
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    #[inline]
    pub fn get(&self, id: T::Id) -> Result<T> {
        Ok(self.scan()?.find(|entry| Ok(entry.id() == id))?.expect("got invalid oid"))
    }

    #[inline]
    pub fn find(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        parent: Option<Oid<T::Parent>>,
        name: &str,
    ) -> Result<Option<T>> {
        self.scan()?.find(|entry| {
            Ok(entry.parent_oid(catalog, tx)? == parent
                && entry.name(catalog, tx)?.as_str() == name)
        })
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

    #[inline]
    pub fn delete(&mut self, key: impl IntoTuple) -> Result<bool> {
        Ok(self.storage.delete(key)?)
    }
}
