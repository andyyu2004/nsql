#![deny(rust_2018_idioms)]

use std::ops::RangeBounds;
use std::path::Path;

use heed::types::ByteSlice;
use heed::EnvFlags;
use nsql_storage_engine::{
    fallible_iterator, KeyExists, Range, ReadOrWriteTransactionRef, ReadTree, StorageEngine,
    Transaction, TransactionRef, WriteTree,
};

type Result<T, E = heed::Error> = std::result::Result<T, E>;

type UntypedDatabase = heed::Database<ByteSlice, ByteSlice>;

#[derive(Clone, Debug)]
pub struct LmdbStorageEngine {
    env: heed::Env,
}

pub struct ReadonlyTx<'env>(heed::RoTxn<'env>);

pub struct ReadWriteTx<'env>(heed::RwTxn<'env>);

impl StorageEngine for LmdbStorageEngine {
    type Error = heed::Error;

    type Bytes<'txn> = &'txn [u8];

    type Transaction<'env> = ReadonlyTx<'env>;

    type WriteTransaction<'env> = ReadWriteTx<'env>;

    type ReadTree<'env, 'txn> = LmdbReadTree<'env, 'txn> where 'env: 'txn;

    type WriteTree<'env, 'txn> = LmdbWriteTree<'env, 'txn> where 'env: 'txn;

    fn create(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Self::open(path)
    }

    #[inline]
    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // large value `max_readers` has a performance issues so I don't think having a lmdb database per table is practical.
        // Perhaps we can do a lmdb database per schema and have a reasonable limit on it (say ~100)
        // TODO have a look at EnvFlags::WRITE_MAP
        let env =
            unsafe { heed::EnvOpenOptions::new().flags(EnvFlags::NO_SUB_DIR | EnvFlags::NO_TLS) }
                .map_size(2 * 1024 * 1024 * 1024) // 2 GiB
                .max_dbs(2000)
                .open(path)?;
        Ok(Self { env })
    }

    #[inline]
    fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.env.read_txn()?;
        Ok(ReadonlyTx(tx))
    }

    #[inline]
    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, Self::Error> {
        let inner = self.env.write_txn()?;
        Ok(ReadWriteTx(inner))
    }

    #[inline]
    fn open_tree<'env, 'txn>(
        &self,
        txn: &'txn dyn TransactionRef<'env, Self>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        let txn = match txn.as_read_or_write_ref() {
            ReadOrWriteTransactionRef::Read(txn) => &txn.0,
            ReadOrWriteTransactionRef::Write(txn) => &txn.0,
        };
        Ok(self.env.open_database(txn, Some(name))?.map(|db| LmdbReadTree { db, txn }))
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
        let db = self.env.create_database(&txn.0, Some(name))?;
        Ok(LmdbWriteTree { db, txn })
    }

    #[inline]
    fn drop_tree(&self, txn: &Self::WriteTransaction<'_>, name: &str) -> Result<(), Self::Error> {
        self.open_write_tree(txn, name)?.db.drop(&txn.0)?;
        Ok(())
    }
}

pub struct LmdbReadTree<'env, 'txn> {
    db: UntypedDatabase,
    txn: &'txn heed::RoTxn<'env>,
}

unsafe impl Send for LmdbReadTree<'_, '_> {}

unsafe impl Sync for LmdbReadTree<'_, '_> {}

pub struct LmdbWriteTree<'env, 'txn> {
    db: UntypedDatabase,
    txn: &'txn ReadWriteTx<'env>,
}

unsafe impl Send for LmdbWriteTree<'_, '_> {}

impl<'env, 'txn> ReadTree<'env, 'txn, LmdbStorageEngine> for LmdbReadTree<'env, 'txn> {
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<<LmdbStorageEngine as StorageEngine>::Bytes<'a>>, heed::Error> {
        self.db.get(self.txn, key)
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .range(self.txn, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .rev_range(self.txn, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }
}

impl<'env, 'txn> ReadTree<'env, 'txn, LmdbStorageEngine> for LmdbWriteTree<'env, 'txn> {
    #[inline]
    fn get<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<
        Option<<LmdbStorageEngine as StorageEngine>::Bytes<'a>>,
        <LmdbStorageEngine as StorageEngine>::Error,
    > {
        self.db.get(&self.txn.0, key)
    }

    #[inline]
    fn range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .range(&self.txn.0, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }

    #[inline]
    fn rev_range<'a>(
        &'a self,
        range: impl RangeBounds<[u8]> + 'a,
    ) -> Result<Range<'a, LmdbStorageEngine>, heed::Error> {
        self.db
            .rev_range(&self.txn.0, &range)
            .map(fallible_iterator::convert)
            .map(|iter| Box::new(iter) as _)
    }
}

impl<'env, 'txn> WriteTree<'env, 'txn, LmdbStorageEngine> for LmdbWriteTree<'env, 'txn> {
    #[inline]
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<Result<(), KeyExists>, heed::Error> {
        match self.db.put(&self.txn.0, key, value) {
            Ok(()) => Ok(Ok(())),
            Err(err) => match err {
                heed::Error::Mdb(heed::MdbError::KeyExist) => Ok(Err(KeyExists)),
                _ => Err(err),
            },
        }
    }

    #[inline]
    fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), heed::Error> {
        self.db.update(&self.txn.0, key, value)
    }

    #[inline]
    fn delete(&mut self, key: &[u8]) -> Result<bool, heed::Error> {
        self.db.delete(&self.txn.0, key)
    }
}

impl<'env> TransactionRef<'env, LmdbStorageEngine> for ReadonlyTx<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, LmdbStorageEngine> {
        ReadOrWriteTransactionRef::Read(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn TransactionRef<'env, LmdbStorageEngine> {
        self
    }
}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadonlyTx<'env> {
    #[inline]
    fn commit(self) -> Result<(), heed::Error> {
        Ok(())
    }

    #[inline]
    fn abort(self) -> Result<(), heed::Error> {
        Ok(())
    }
}

impl<'env> TransactionRef<'env, LmdbStorageEngine> for ReadWriteTx<'env> {
    #[inline]
    fn as_read_or_write_ref(&self) -> ReadOrWriteTransactionRef<'env, '_, LmdbStorageEngine> {
        ReadOrWriteTransactionRef::Write(self)
    }

    #[inline]
    fn as_dyn(&self) -> &dyn TransactionRef<'env, LmdbStorageEngine> {
        self
    }
}

impl<'env> Transaction<'env, LmdbStorageEngine> for ReadWriteTx<'env> {
    #[inline]
    fn commit(self) -> Result<(), heed::Error> {
        self.0.commit()
    }

    #[inline]
    fn abort(self) -> Result<(), heed::Error> {
        self.0.abort();
        Ok(())
    }
}
