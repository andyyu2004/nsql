#![deny(rust_2018_idioms)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_projections)]
#![feature(bound_map)]

use std::ops::{Bound, Deref, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use nsql_storage_engine::{fallible_iterator, Range, ReadOrWriteTransactionRef};
use redb::{AccessGuard, ReadableTable, TableHandle};

type Result<T, E = redb::Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RedbStorageEngine {
    db: redb::Database,
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
    type Error = redb::Error;

    type Bytes<'txn> = AccessGuardDerefWrapper<'txn>;

    type Transaction<'env> = ReadTransaction<'env>;

    type WriteTransaction<'env> = Transaction<'env>;

    type ReadTree<'env, 'txn> = Box<dyn ReadableTableDyn + 'txn> where 'env: 'txn;

    type WriteTree<'env, 'txn> = redb::Table<'env, 'txn, &'static [u8], &'static [u8]> where 'env: 'txn;

    #[inline]
    fn create(path: impl AsRef<Path>) -> std::result::Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(redb::Database::create(path).map(|db| Self { db })?)
    }

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(redb::Database::open(path).map(|db| Self { db })?)
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
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn dyn nsql_storage_engine::Transaction<'env, Self>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        match txn.as_read_or_write_ref() {
            ReadOrWriteTransactionRef::Read(txn) => {
                match txn.0.open_table(redb::TableDefinition::new(name)) {
                    Ok(table) => Ok(Some(Box::new(table))),
                    Err(redb::TableError::TableDoesNotExist(_)) => unreachable!(),
                    Err(e) => Err(e)?,
                }
            }
            ReadOrWriteTransactionRef::Write(txn) => {
                match txn.0.open_table(redb::TableDefinition::new(name)) {
                    Ok(table) => Ok(Some(Box::new(table))),
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
                    Err(e) => Err(e)?,
                }
            }
        }
    }

    #[inline]
    fn open_write_tree<'env, 'txn>(
        &self,
        txn: &'txn Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        match txn.0.open_table(redb::TableDefinition::new(name)) {
            Ok(table) => Ok(table),
            Err(redb::TableError::TableDoesNotExist(_)) => unreachable!(),
            Err(e) => Err(e)?,
        }
    }

    fn drop_tree(&self, txn: &Self::WriteTransaction<'_>, name: &str) -> Result<(), Self::Error> {
        let ok = txn.0.delete_table(redb::TableDefinition::<(), ()>::new(name))?;
        if !ok {
            let tables =
                txn.0.list_tables()?.map(|handle| handle.name().to_string()).collect::<Vec<_>>();
            panic!("attempted to drop non-existent table `{name}`, available tables {tables:?}");
        }
        Ok(())
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
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> std::result::Result<
        Option<<RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'a>>,
        redb::Error,
    > {
        Ok(ReadableTable::get(self, key).map(|v| v.map(AccessGuardDerefWrapper))?)
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, RedbStorageEngine>, redb::Error> {
        Ok(Box::new(fallible_iterator::convert(
            ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?
                .map(|kv| {
                    kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))
                        .map_err(Into::into)
                })
                .rev(),
        )))
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, RedbStorageEngine>, redb::Error> {
        Ok(Box::new(fallible_iterator::convert(
            ReadableTable::range::<&[u8]>(self, (range.start_bound(), range.end_bound()))?.map(
                |kv| {
                    kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))
                        .map_err(Into::into)
                },
            ),
        )))
    }
}

impl<'env, 'txn> nsql_storage_engine::WriteTree<'env, 'txn, RedbStorageEngine>
    for redb::Table<'env, 'txn, &[u8], &[u8]>
{
    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), redb::Error> {
        // can return a bool if we need to know if the key was already present
        self.insert(key, value).map(|prev| prev.is_none())?;
        Ok(())
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<bool, redb::Error> {
        Ok(self.remove(key)?.is_some())
    }
}

impl<'env> nsql_storage_engine::Transaction<'env, RedbStorageEngine> for ReadTransaction<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, RedbStorageEngine> {
        ReadOrWriteTransactionRef::Read(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn nsql_storage_engine::Transaction<'env, RedbStorageEngine> {
        self
    }
}

impl<'env> nsql_storage_engine::Transaction<'env, RedbStorageEngine> for Transaction<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, RedbStorageEngine> {
        ReadOrWriteTransactionRef::Write(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn nsql_storage_engine::Transaction<'env, RedbStorageEngine> {
        self
    }
}

impl<'env> nsql_storage_engine::WriteTransaction<'env, RedbStorageEngine> for Transaction<'env> {
    #[inline]
    fn commit(self) -> Result<(), redb::Error> {
        Ok(self.0.commit()?)
    }

    #[inline]
    fn abort(self) -> Result<(), redb::Error> {
        Ok(self.0.abort()?)
    }
}

pub trait ReadableTableDyn: Send + Sync {
    // Required methods
    fn get(&self, key: &[u8]) -> Result<Option<AccessGuard<'_, &'static [u8]>>, redb::Error>;

    fn range(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<redb::Range<'_, &'static [u8], &'static [u8]>, redb::Error>;

    fn len(&self) -> Result<u64, redb::Error>;

    fn is_empty(&self) -> Result<bool, redb::Error>;

    fn iter(&self) -> Result<redb::Range<'_, &'static [u8], &'static [u8]>, redb::Error>;
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> ReadableTableDyn for T
where
    T: Send + Sync,
{
    fn get(&self, key: &[u8]) -> Result<Option<AccessGuard<'_, &'static [u8]>>, redb::Error> {
        Ok(ReadableTable::get(self, key)?)
    }

    #[inline]
    fn range(
        &self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
    ) -> Result<redb::Range<'_, &'static [u8], &'static [u8]>, redb::Error> {
        Ok(ReadableTable::range::<&[u8]>(self, range)?)
    }

    #[inline]
    fn len(&self) -> Result<u64, redb::Error> {
        Ok(ReadableTable::len(self)?)
    }

    #[inline]
    fn is_empty(&self) -> Result<bool, redb::Error> {
        Ok(ReadableTable::is_empty(self)?)
    }

    #[inline]
    fn iter(&self) -> Result<redb::Range<'_, &'static [u8], &'static [u8]>, redb::Error> {
        Ok(ReadableTable::iter(self)?)
    }
}

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, RedbStorageEngine>
    for Box<dyn ReadableTableDyn + 'txn>
{
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> std::result::Result<
        Option<<RedbStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'a>>,
        redb::Error,
    > {
        (**self).get(key).map(|v| v.map(AccessGuardDerefWrapper))
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, RedbStorageEngine>, redb::Error> {
        Ok(Box::new(fallible_iterator::convert(
            (**self).range((range.start_bound(), range.end_bound()))?.map(|kv| {
                kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))
                    .map_err(Into::into)
            }),
        )))
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, RedbStorageEngine>, redb::Error> {
        Ok(Box::new(fallible_iterator::convert(
            (**self).range((range.start_bound(), range.end_bound()))?.map(|kv| {
                kv.map(|(k, v)| (AccessGuardDerefWrapper(k), AccessGuardDerefWrapper(v)))
                    .map_err(Into::into)
            }),
        )))
    }
}
