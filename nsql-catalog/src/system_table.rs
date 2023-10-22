use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_core::Oid;
use nsql_storage::eval::FunctionCatalog;
use nsql_storage::tuple::{FromTuple, IntoTuple};
use nsql_storage_engine::{
    ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine, Transaction,
};

use crate::entity::table::{PrimaryKeyConflict, TableStorage};
use crate::{Catalog, Result, SystemEntity, Table};

#[repr(transparent)]
pub struct SystemTableView<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T> {
    storage: TableStorage<'env, 'txn, S, M>,
    phantom: PhantomData<T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    pub fn new(catalog: Catalog<'env, S>, tx: M::TransactionRef<'txn>) -> Result<Self> {
        // we need to view the tables in bootstrap mode to avoid a cycle
        let table =
            SystemTableView::<S, M, Table>::new_bootstrap(catalog.storage(), tx)?.get(T::table())?;
        Ok(Self { storage: table.storage(catalog, tx)?, phantom: PhantomData })
    }

    pub(crate) fn new_bootstrap(
        storage: &'env S,
        tx: M::TransactionRef<'txn>,
    ) -> Result<Self, S::Error> {
        // todo indexes
        let storage = TableStorage::<'env, 'txn, S, M>::open(
            storage,
            tx,
            T::bootstrap_table_storage_info(),
            // no access to table indexes during bootstrap
            vec![],
        )?;

        Ok(Self { storage, phantom: PhantomData })
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    #[inline]
    pub fn get(&self, key: T::Key) -> Result<T> {
        self.scan()?
            .find(|entry| Ok(entry.key() == key))?
            .ok_or_else(|| anyhow!("got invalid key for {}: `{:?}", T::desc(), key))
    }

    #[inline]
    pub fn find(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        parent: Option<Oid<T::Parent>>,
        key: &T::SearchKey,
    ) -> Result<Option<T>> {
        self.scan()?.find(|entry| {
            Ok(entry.parent_oid(catalog, tx)? == parent && &entry.search_key() == key)
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
    pub fn insert(
        &mut self,
        catalog: &dyn FunctionCatalog<'env, S, ReadWriteExecutionMode>,
        tx: &S::WriteTransaction<'env>,
        value: T,
    ) -> Result<()> {
        self.storage.insert(catalog, tx, &value.into_tuple())?.map_err(
            |PrimaryKeyConflict { key }| {
                let typed_key = T::Key::from_tuple(key)
                    .expect("this shouldn't fail as we know the expected shape");
                anyhow::anyhow!(
                    "primary key conflict for {}: {:?} already exists",
                    T::desc(),
                    typed_key
                )
            },
        )
    }

    #[inline]
    pub fn delete(&mut self, key: impl IntoTuple) -> Result<bool> {
        Ok(self.storage.delete(key)?)
    }
}
