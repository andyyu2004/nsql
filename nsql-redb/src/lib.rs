#![deny(rust_2018_idioms)]
#![feature(type_alias_impl_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]
#![feature(bound_map)]

use std::ops::{Deref, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use redb::{AccessGuard, ReadableTable};

type Result<T, E = redb::Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct RedbStorageEngine {
    db: Arc<redb::Database>,
}

#[derive(Clone)]
pub struct ReadTransaction<'env>(Arc<redb::ReadTransaction<'env>>);

pub struct Transaction<'env>(redb::WriteTransaction<'env>);

impl<'env> Deref for Transaction<'env> {
    type Target = ReadTransaction<'env>;

    fn deref(&self) -> &Self::Target {
        unsafe { std::mem::transmute(self) }
    }
}

impl nsql_storage_engine::StorageEngine for RedbStorageEngine {
    type Bytes<'txn> = AccessGuardDerefWrapper<'txn>;

    type Error = redb::Error;

    type Transaction<'env> = ReadTransaction<'env>;

    type WriteTransaction<'env> = Transaction<'env>;

    type ReadTree<'env, 'txn> = redb::ReadOnlyTable<'txn, &'static [u8], &'static [u8]> where 'env: 'txn;

    type WriteTree<'env, 'txn> = redb::Table<'env, 'txn, &'static [u8], &'static [u8]> where 'env: 'txn;

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        redb::Database::open(path).map(Arc::new).map(|db| Self { db })
    }

    #[inline]
    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.db.begin_read()?;
        Ok(ReadTransaction(Arc::new(tx)))
    }

    #[inline]
    fn begin_write(&self) -> std::result::Result<Self::WriteTransaction<'_>, Self::Error> {
        let tx = self.db.begin_write()?;
        Ok(Transaction(tx))
    }

    #[inline]
    fn open_tree_readonly<'env, 'txn>(
        &self,
        txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        match txn.0.open_table(redb::TableDefinition::new(name)) {
            Ok(table) => Ok(Some(table)),
            Err(redb::Error::TableDoesNotExist(_)) => unreachable!(),
            Err(e) => Err(e),
        }
    }

    #[inline]
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        match txn.0.open_table(redb::TableDefinition::new(name)) {
            Ok(table) => Ok(table),
            Err(redb::Error::TableDoesNotExist(_)) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, RedbStorageEngine>
    for redb::ReadOnlyTable<'txn, &[u8], &[u8]>
{
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
}

impl<'env, 'txn> nsql_storage_engine::WriteTree<'env, 'txn, RedbStorageEngine>
    for redb::Table<'env, 'txn, &[u8], &[u8]>
{
}

impl<'env> nsql_storage_engine::Transaction<'env, RedbStorageEngine> for ReadTransaction<'env> {
    #[inline]
    fn get<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        key: &[u8],
    ) -> std::result::Result<
        Option<<RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>>,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::get(tree, key)?.map(AccessGuardDerefWrapper))
    }

    #[inline]
    fn range<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> std::result::Result<
        impl Iterator<
            Item = std::result::Result<
                (
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                ),
                <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
            >,
        >,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::range::<&[u8]>(tree, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))))
    }

    #[inline]
    fn rev_range<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> std::result::Result<
        impl Iterator<
            Item = std::result::Result<
                (
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                ),
                <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
            >,
        >,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::range::<&[u8]>(tree, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v))))
            .rev())
    }
}

impl<'env> nsql_storage_engine::Transaction<'env, RedbStorageEngine> for Transaction<'env> {
    #[inline]
    fn get<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        key: &[u8],
    ) -> std::result::Result<
        Option<<RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>>,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::get(tree, key)?.map(AccessGuardDerefWrapper))
    }

    #[inline]
    fn range<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> std::result::Result<
        impl Iterator<
            Item = std::result::Result<
                (
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                ),
                <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
            >,
        >,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::range::<&[u8]>(tree, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))))
    }

    #[inline]
    fn rev_range<'txn>(
        &'txn self,
        tree: &'txn <RedbStorageEngine as nsql_storage_engine::StorageEngine>::ReadTree<'env, 'txn>,
        range: impl RangeBounds<[u8]> + 'txn,
    ) -> std::result::Result<
        impl Iterator<
            Item = std::result::Result<
                (
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                    <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'txn>,
                ),
                <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
            >,
        >,
        <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error,
    > {
        Ok(ReadableTable::range::<&[u8]>(tree, (range.start_bound(), range.end_bound()))?
            .map(|kv| kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v))))
            .rev())
    }
}

impl<'env> nsql_storage_engine::WriteTransaction<'env, RedbStorageEngine> for Transaction<'env> {
    #[inline]
    fn commit(self) -> Result<(), redb::Error> {
        self.0.commit()
    }

    #[inline]
    fn rollback(self) -> Result<(), redb::Error> {
        self.0.abort()
    }

    #[inline]
    fn put<'txn>(
        &'txn mut self,
        tree: &mut <RedbStorageEngine as nsql_storage_engine::StorageEngine>::WriteTree<'env, 'txn>,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error> {
        tree.insert(key, value).map(|prev| prev.is_none())
    }

    fn delete<'txn>(
        &mut self,
        tree: &mut <RedbStorageEngine as nsql_storage_engine::StorageEngine>::WriteTree<'env, 'txn>,
        key: &[u8],
    ) -> std::result::Result<bool, <RedbStorageEngine as nsql_storage_engine::StorageEngine>::Error>
    {
        Ok(tree.remove(key)?.is_some())
    }
}
