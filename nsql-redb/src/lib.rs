use std::path::Path;

type Result<T, E = redb::Error> = std::result::Result<T, E>;

pub struct LmdbStorageEngine {
    db: redb::Database,
}

pub struct ReadTransaction<'db> {
    tx: redb::ReadTransaction<'db>,
}

pub struct Transaction<'db> {
    tx: redb::WriteTransaction<'db>,
}

impl nsql_storage_engine::StorageEngine for LmdbStorageEngine {
    type Error = redb::Error;

    type ReadTransaction<'db> = ReadTransaction<'db>;

    type Transaction<'db> = Transaction<'db>;

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
        Ok(ReadTransaction { tx })
    }

    #[inline]
    fn begin(&self) -> std::result::Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.db.begin_write()?;
        Ok(Transaction { tx })
    }
}

impl<'env> nsql_storage_engine::ReadTransaction for ReadTransaction<'env> {
    type Error = redb::Error;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error> {
        todo!()
    }
}

impl<'env> nsql_storage_engine::ReadTransaction for Transaction<'env> {
    type Error = redb::Error;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error> {
        todo!()
    }
}

impl<'env> nsql_storage_engine::Transaction for Transaction<'env> {
    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }
}
