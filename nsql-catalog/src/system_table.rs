use std::hash::BuildHasherDefault;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::anyhow;
use dashmap::DashMap;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use nsql_core::Oid;
use nsql_storage::tuple::{FromFlatTuple, IntoFlatTuple};
use nsql_storage_engine::{ExecutionMode, FallibleIterator, ReadWriteExecutionMode, StorageEngine};
use rustc_hash::FxHasher;

use crate::entity::table::{PrimaryKeyConflict, TableStorage};
use crate::{Catalog, FunctionCatalog, Result, SystemEntity, Table, TransactionContext};

pub struct SystemTableView<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
{
    storage: TableStorage<'env, 'txn, S, M>,
    cache: DashMap<T::Key, T, BuildHasherDefault<FxHasher>>,
    phantom: PhantomData<T>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    pub fn new(
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Self> {
        // we need to view the tables in bootstrap mode to avoid a cycle
        let table =
            SystemTableView::<S, M, Table>::new_bootstrap(catalog.storage(), tx)?.get(T::TABLE)?;
        Ok(Self {
            storage: table.storage(catalog, tx)?,
            cache: Default::default(),
            phantom: PhantomData,
        })
    }

    #[track_caller]
    pub(crate) fn new_bootstrap(
        storage: &'env S,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Result<Self, S::Error> {
        // todo indexes
        let storage = TableStorage::<'env, 'txn, S, M>::open(
            storage,
            tx,
            T::bootstrap_table_storage_info(),
            // no access to table indexes during bootstrap
            vec![],
        )?;

        Ok(Self { storage, cache: Default::default(), phantom: PhantomData })
    }
}

#[fix_hidden_lifetime_bug]
impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: SystemEntity>
    SystemTableView<'env, 'txn, S, M, T>
{
    #[inline]
    pub fn get(&self, key: T::Key) -> Result<T> {
        let f = |key: T::Key| -> Result<T> {
            self.storage
                .get(key)?
                .ok_or_else(|| anyhow!("got invalid key for {}: `{:?}", T::desc(), key))
                .and_then(|tuple| Ok(T::from_tuple(tuple)?))
        };

        if M::READONLY {
            self.cache.entry(key).or_try_insert_with(|| f(key)).map(|v| v.value().clone())
        } else {
            f(key)
        }
    }

    #[inline]
    pub fn find(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        parent: Option<Oid<T::Parent>>,
        key: &T::SearchKey,
    ) -> Result<Option<T>> {
        self.scan(..)?.find(|entry| {
            Ok(entry.parent_oid(catalog, tx)? == parent && &entry.search_key() == key)
        })
    }

    #[inline]
    #[fix_hidden_lifetime_bug]
    pub fn scan<'a>(
        &'a self,
        bounds: impl RangeBounds<[u8]> + 'a,
    ) -> Result<impl FallibleIterator<Item = T, Error = anyhow::Error> + 'a> {
        Ok(self
            .storage
            .scan(bounds, None)?
            .map_err(Into::into)
            .map(|tuple| Ok(T::from_tuple(tuple)?)))
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
        catalog: &dyn FunctionCatalog<'env, 'txn, S, ReadWriteExecutionMode>,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        value: T,
    ) -> Result<()> {
        let tuple = value.into_tuple();
        self.storage.insert(catalog, tx, &tuple)?.map_err(|PrimaryKeyConflict { key }| {
            let typed_key = T::Key::from_tuple(key).unwrap_or_else(|err| {
                panic!(
                    "this shouldn't fail as we know the expected shape {}: {err}",
                    std::any::type_name::<T>()
                )
            });
            anyhow::anyhow!(
                "primary key conflict for {}: {:?} already exists",
                T::desc(),
                typed_key
            )
        })
    }

    #[inline]
    pub fn delete(&mut self, key: impl IntoFlatTuple) -> Result<bool> {
        Ok(self.storage.delete(key)?)
    }
}
