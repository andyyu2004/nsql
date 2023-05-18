#![deny(rust_2018_idioms)]
#![feature(type_alias_impl_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]
#![feature(bound_map)]

use std::ops::{Deref, RangeBounds};
use std::path::Path;

use redb::{AccessGuard, ReadableTable};

type Result<T, E = redb::Error> = std::result::Result<T, E>;

pub struct RedbStorageEngine {
    db: redb::Database,
}

pub struct ReadTransaction<'db>(redb::ReadTransaction<'db>);

pub struct Transaction<'db>(redb::WriteTransaction<'db>);

impl<'db> Deref for Transaction<'db> {
    type Target = ReadTransaction<'db>;

    fn deref(&self) -> &Self::Target {
        unsafe { std::mem::transmute(self) }
    }
}

impl nsql_storage_engine::StorageEngine for RedbStorageEngine {
    type Error = redb::Error;

    type ReadTransaction<'db> = ReadTransaction<'db>;

    type Transaction<'db> = Transaction<'db>;

    type ReadTree<'env, 'txn> = redb::ReadOnlyTable<'txn, &'static [u8], &'static [u8]> where 'env: 'txn;

    type Tree<'env, 'txn> = redb::Table<'env, 'txn, &'static [u8], &'static [u8]> where 'env: 'txn;

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        redb::Database::open(path).map(|db| Self { db })
    }

    #[inline]
    fn begin_readonly(&self) -> Result<Self::ReadTransaction<'_>, Self::Error> {
        let tx = self.db.begin_read()?;
        Ok(ReadTransaction(tx))
    }

    #[inline]
    fn begin(&self) -> std::result::Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.db.begin_write()?;
        Ok(Transaction(tx))
    }

    #[inline]
    fn open_tree_readonly<'env, 'txn>(
        &self,
        txn: &'env Self::ReadTransaction<'txn>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        match txn.0.open_table(redb::TableDefinition::new(name)) {
            Ok(table) => Ok(Some(table)),
            Err(redb::Error::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[inline]
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::Tree<'env, 'txn>>, Self::Error> {
        match txn.0.open_table(redb::TableDefinition::new(name)) {
            Ok(table) => Ok(Some(table)),
            Err(redb::Error::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, RedbStorageEngine>
    for redb::ReadOnlyTable<'txn, &[u8], &[u8]>
{
    type Bytes = AccessGuardDerefWrapper<'txn>;

    #[inline]
    fn get(
        &'txn self,
        _txn: &'txn ReadTransaction<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, redb::Error> {
        Ok(ReadableTable::get(self, key)?.map(AccessGuardDerefWrapper))
    }

    #[inline]
    fn range(
        &'txn self,
        _txn: &'txn ReadTransaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), redb::Error>>> {
        Ok(ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))))
    }

    #[inline]
    fn rev_range(
        &'txn self,
        _txn: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTransaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), redb::Error>>, redb::Error>
    {
        Ok(ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v))))
            .rev())
    }
}

pub struct AccessGuardDerefWrapper<'a>(AccessGuard<'a, &'a [u8]>);

impl<'a> Deref for AccessGuardDerefWrapper<'a> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.value()
    }
}

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, RedbStorageEngine>
    for redb::Table<'env, 'txn, &[u8], &[u8]>
{
    type Bytes = AccessGuardDerefWrapper<'txn>;

    #[inline]
    fn get(
        &'txn self,
        _txn: &'txn ReadTransaction<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, redb::Error> {
        Ok(ReadableTable::get(self, key)?.map(AccessGuardDerefWrapper))
    }

    #[inline]
    fn range(
        &'txn self,
        _txn: &'txn ReadTransaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), redb::Error>>, redb::Error>
    {
        Ok(ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))))
    }

    #[inline]
    fn rev_range(
        &'txn self,
        _txn: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTransaction<'_>,
        range: impl RangeBounds<[u8]>,
    ) -> Result<impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), redb::Error>>, redb::Error>
    {
        Ok(ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v))))
            .rev())
    }
}

impl<'env, 'txn> nsql_storage_engine::Tree<'env, 'txn, RedbStorageEngine>
    for redb::Table<'env, 'txn, &[u8], &[u8]>
{
    #[inline]
    fn put(
        &mut self,
        _txn: &mut Transaction<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), redb::Error> {
        self.insert(key, value)?;
        Ok(())
    }

    #[inline]
    fn delete(&mut self, _txn: &mut Transaction<'_>, key: &[u8]) -> Result<bool, redb::Error> {
        Ok(self.remove(key)?.is_some())
    }
}

impl<'env> nsql_storage_engine::ReadTransaction for ReadTransaction<'env> {
    type Error = redb::Error;
}

impl<'env> nsql_storage_engine::ReadTransaction for Transaction<'env> {
    type Error = redb::Error;
}

impl<'env> nsql_storage_engine::Transaction for Transaction<'env> {}
