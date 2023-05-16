#![deny(rust_2018_idioms)]
use std::path::Path;

use heed::UntypedDatabase;
use nsql_storage_engine::{Database, ReadTransaction, StorageEngine, Transaction};

type Result<T, E = heed::Error> = std::result::Result<T, E>;

pub struct LmdbStorageEngine {
    env: heed::Env,
    db: UntypedDatabase,
}

pub struct ReadonlyTx<'env> {
    db: UntypedDatabase,
    tx: heed::RoTxn<'env>,
}

pub struct ReadWriteTx<'env> {
    db: UntypedDatabase,
    tx: heed::RwTxn<'env, 'env>,
}

impl StorageEngine for LmdbStorageEngine {
    type Error = heed::Error;

    type ReadTransaction<'env> = ReadonlyTx<'env>;

    type Transaction<'env> = ReadWriteTx<'env>;

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // large value `max_readers` has a performance issues so I don't think having a lmdb database per table is practical.
        // Perhaps we can do a lmdb database per schema and have a reasonable limit on it (say ~100)
        std::fs::OpenOptions::new().create(true).write(true).truncate(false).open(&path)?;
        let env = unsafe { heed::EnvOpenOptions::new().flag(heed::flags::Flags::MdbNoSubDir) }
            .open(path)?;
        let db = env.open_database(None)?.expect("main database should exist");
        Ok(Self { db, env })
    }

    #[inline]
    fn begin_readonly(&self) -> Result<Self::ReadTransaction<'_>, Self::Error> {
        let tx = self.env.read_txn()?;
        Ok(ReadonlyTx { db: self.db, tx })
    }

    #[inline]
    fn begin(&self) -> std::result::Result<Self::Transaction<'_>, Self::Error> {
        let inner = self.env.write_txn()?;
        Ok(ReadWriteTx { db: self.db, tx: inner })
    }
}

impl Database<LmdbStorageEngine> for heed::UntypedDatabase {
    fn get<'tx>(
        &self,
        txn: &'tx ReadonlyTx<'_>,
        key: &[u8],
    ) -> Result<Option<&'tx [u8]>, heed::Error> {
        self.get(&txn.tx, key)
    }

    fn put(&self, txn: &mut ReadWriteTx<'_>, key: &[u8], value: &[u8]) -> Result<(), heed::Error> {
        self.put(&mut txn.tx, key, value)
    }

    fn delete(&self, txn: &mut ReadWriteTx<'_>, key: &[u8]) -> Result<(), heed::Error> {
        self.delete(&mut txn.tx, key)?;
        Ok(())
    }
}

impl<'env> ReadTransaction for ReadonlyTx<'env> {
    type Error = heed::Error;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error> {
        self.db.get(&self.tx, key)
    }
}

impl<'env> ReadTransaction for ReadWriteTx<'env> {
    type Error = heed::Error;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error> {
        self.db.get(&self.tx, key)
    }
}

impl<'env> Transaction for ReadWriteTx<'env> {
    #[inline]
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.db.put(&mut self.tx, key, value)
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<(), Self::Error> {
        self.db.delete(&mut self.tx, key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
