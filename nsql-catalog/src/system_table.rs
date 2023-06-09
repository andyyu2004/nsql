use std::marker::PhantomData;
use std::sync::Arc;

use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_storage::tuple::FromTuple;
use nsql_storage::{TableStorage, TableStorageInfo};
use nsql_storage_engine::{FallibleIterator, ReadonlyExecutionMode, StorageEngine};

use crate::Result;

pub struct SystemTableView<'env, S, T> {
    storage: &'env S,
    storage_info: TableStorageInfo,
    phantom: PhantomData<T>,
}

impl<'env, S: StorageEngine, T: FromTuple> SystemTableView<'env, S, T> {
    pub fn new(storage: &'env S, storage_info: TableStorageInfo) -> Self {
        Self { storage, storage_info, phantom: PhantomData }
    }

    #[fix_hidden_lifetime_bug]
    pub fn scan<'txn>(
        &self,
        tx: &'txn S::Transaction<'txn>,
    ) -> Result<impl FallibleIterator<Item = T, Error = anyhow::Error> + 'txn> {
        let storage = Arc::new(TableStorage::<S, ReadonlyExecutionMode<S>>::open(
            &self.storage,
            tx,
            self.storage_info.clone(),
        )?);

        Ok(storage.scan(None)?.map_err(Into::into).map(|tuple| Ok(T::from_tuple(tuple)?)))
    }
}
