#![deny(rust_2018_idioms)]

use std::ops::Deref;
use std::path::Path;

use redb::ReadableTable;

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
    #[inline]
    fn get(
        &self,
        txn: &'txn ReadTransaction<'_>,
        key: &[u8],
    ) -> Result<Option<&'txn [u8]>, redb::Error> {
        ReadableTable::get(self, &txn.0, key)
    }
}

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, RedbStorageEngine>
    for redb::Table<'env, 'txn, &[u8], &[u8]>
{
    #[inline]
    fn get(
        &self,
        txn: &'txn ReadTransaction<'_>,
        key: &[u8],
    ) -> Result<Option<&'txn [u8]>, redb::Error> {
        todo!()
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
