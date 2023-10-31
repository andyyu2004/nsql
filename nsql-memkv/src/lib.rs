#![allow(dead_code)] // for now

use std::ops::RangeBounds;
use std::path::Path;

#[derive(Debug)]
pub struct MemStorageEngine(memkv::Env);

impl nsql_storage_engine::StorageEngine for MemStorageEngine {
    type Error = memkv::Error;

    type Bytes<'txn> = &'txn [u8];

    type Transaction<'env> = ReadTransaction<'env>;

    type WriteTransaction<'env> = WriteTransaction<'env>;

    type ReadTree<'env: 'txn, 'txn> = ReadTree<'env, 'txn>;

    type WriteTree<'env: 'txn, 'txn> = WriteTree<'env, 'txn>;

    fn create(_path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn open(_path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        todo!()
    }

    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        todo!()
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, Self::Error> {
        todo!()
    }

    fn open_tree<'env, 'txn>(
        &self,
        _txn: &'txn dyn nsql_storage_engine::TransactionRef<'env, Self>,
        _name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        todo!()
    }

    fn open_write_tree<'env, 'txn>(
        &self,
        _txn: &'txn Self::WriteTransaction<'env>,
        _name: &str,
    ) -> Result<Self::WriteTree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        todo!()
    }

    fn drop_tree(&self, _txn: &Self::WriteTransaction<'_>, _name: &str) -> Result<(), Self::Error> {
        todo!()
    }
}

pub struct ReadTree<'env, 'txn>(memkv::ReadTable<'env, 'txn>);

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, MemStorageEngine>
    for ReadTree<'env, 'txn>
{
    fn get<'a>(
        &'a self,
        _key: &[u8],
    ) -> Result<
        Option<<MemStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'a>>,
        memkv::Error,
    > {
        todo!()
    }

    fn range<'a>(
        &'a self,
        _range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<nsql_storage_engine::Range<'a, MemStorageEngine>, memkv::Error> {
        todo!()
    }

    fn rev_range<'a>(
        &'a self,
        _range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<nsql_storage_engine::Range<'a, MemStorageEngine>, memkv::Error> {
        todo!()
    }
}

pub struct WriteTree<'env, 'txn>(memkv::WriteTable<'env, 'txn>);

impl<'env, 'txn> nsql_storage_engine::ReadTree<'env, 'txn, MemStorageEngine>
    for WriteTree<'env, 'txn>
{
    fn get<'a>(
        &'a self,
        _key: &[u8],
    ) -> Result<
        Option<<MemStorageEngine as nsql_storage_engine::StorageEngine>::Bytes<'a>>,
        memkv::Error,
    > {
        todo!()
    }

    fn range<'a>(
        &'a self,
        _range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<nsql_storage_engine::Range<'a, MemStorageEngine>, memkv::Error> {
        todo!()
    }

    fn rev_range<'a>(
        &'a self,
        _range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<nsql_storage_engine::Range<'a, MemStorageEngine>, memkv::Error> {
        todo!()
    }
}

impl<'env, 'txn> nsql_storage_engine::WriteTree<'env, 'txn, MemStorageEngine>
    for WriteTree<'env, 'txn>
{
    fn insert(
        &mut self,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<Result<(), nsql_storage_engine::KeyExists>, memkv::Error> {
        todo!()
    }

    fn update(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), memkv::Error> {
        todo!()
    }

    fn delete(&mut self, _key: &[u8]) -> std::result::Result<bool, memkv::Error> {
        todo!()
    }
}

pub struct ReadTransaction<'env>(memkv::ReadTransaction<'env>);

impl<'env> nsql_storage_engine::TransactionRef<'env, MemStorageEngine> for ReadTransaction<'env> {
    fn as_read_or_write_ref(
        &self,
    ) -> nsql_storage_engine::ReadOrWriteTransactionRef<'env, '_, MemStorageEngine> {
        todo!()
    }

    fn as_dyn(&self) -> &dyn nsql_storage_engine::TransactionRef<'env, MemStorageEngine> {
        todo!()
    }
}

impl<'env> nsql_storage_engine::Transaction<'env, MemStorageEngine> for ReadTransaction<'env> {
    fn commit(self) -> Result<(), memkv::Error> {
        todo!()
    }

    fn abort(self) -> Result<(), memkv::Error> {
        todo!()
    }
}

pub struct WriteTransaction<'env>(memkv::WriteTransaction<'env>);

impl<'env> nsql_storage_engine::TransactionRef<'env, MemStorageEngine> for WriteTransaction<'env> {
    fn as_read_or_write_ref(
        &self,
    ) -> nsql_storage_engine::ReadOrWriteTransactionRef<'env, '_, MemStorageEngine> {
        todo!()
    }

    fn as_dyn(&self) -> &dyn nsql_storage_engine::TransactionRef<'env, MemStorageEngine> {
        todo!()
    }
}

impl<'env> nsql_storage_engine::Transaction<'env, MemStorageEngine> for WriteTransaction<'env> {
    fn commit(self) -> Result<(), memkv::Error> {
        todo!()
    }

    fn abort(self) -> Result<(), memkv::Error> {
        todo!()
    }
}
