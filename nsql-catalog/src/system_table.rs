use std::marker::PhantomData;

use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage::{TableStorage, TableStorageInfo};
use nsql_storage_engine::{ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine};

use crate::Result;

pub struct SystemTableView<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T> {
    storage: TableStorage<'env, 'txn, S, M>,
    phantom: PhantomData<T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T>
    SystemTableView<'env, 'txn, S, M, T>
{
    pub fn new(
        storage: &'env S,
        tx: &'txn M::Transaction,
        storage_info: TableStorageInfo,
    ) -> Result<Self, S::Error> {
        let storage = TableStorage::<'env, 'txn, S, M>::open(storage, tx, storage_info)?;

        Ok(Self { storage, phantom: PhantomData })
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T>
    SystemTableView<'env, 'txn, S, M, T>
{
    #[fix_hidden_lifetime_bug]
    pub fn scan(&self) -> Result<impl FallibleIterator<Item = T, Error = anyhow::Error> + '_>
    where
        T: FromTuple,
    {
        Ok(self.storage.scan(None)?.map_err(Into::into).map(|tuple| Ok(T::from_tuple(tuple)?)))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, T>
    SystemTableView<'env, 'txn, S, ReadWriteExecutionMode, T>
{
    pub fn insert(&mut self, value: T) -> Result<()>
    where
        T: IntoTuple,
    {
        self.storage.insert(&value.into_tuple())
    }
}
