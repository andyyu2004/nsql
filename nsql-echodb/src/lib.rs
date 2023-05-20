#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_projections)]

mod echodb;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use nsql_storage_engine::{ReadTree, StorageEngine, Transaction, WriteTransaction, WriteTree};
use parking_lot::RwLock;

use self::echodb::{Db, Key, Tx, Val};

#[derive(Clone)]
pub struct EchoDbEngine {
    db: Arc<RwLock<HashMap<String, Db<Key, Val>>>>,
}

impl StorageEngine for EchoDbEngine {
    type Error = echodb::Error;

    type Transaction<'env> = Arc<Tx<Key, Val>> where Self: 'env;

    type WriteTransaction<'env> = Tx<Key, Val>;

    type ReadTree<'env, 'txn> = Db<Key, Val> where Self: 'env, Self: 'txn, 'env: 'txn;

    type Tree<'env, 'txn> = Db<Key, Val> where Self: 'env, Self: 'txn, 'env: 'txn ;

    fn open(path: impl AsRef<Path>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        assert_eq!(path.as_ref(), Path::new(""), "path must be empty for in memory storage");
        todo!()
    }

    fn begin_readonly(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        todo!()
    }

    fn begin(&self) -> Result<Self::WriteTransaction<'_>, Self::Error> {
        todo!()
    }

    fn open_tree_readonly<'env, 'txn>(
        &self,
        _txn: &'txn Self::Transaction<'env>,
        name: &str,
    ) -> Result<Option<Self::ReadTree<'env, 'txn>>, Self::Error>
    where
        'env: 'txn,
    {
        Ok(self.db.read().get(name).cloned())
    }

    fn open_tree<'env, 'txn>(
        &self,
        _txn: &'txn mut Self::WriteTransaction<'env>,
        name: &str,
    ) -> Result<Self::Tree<'env, 'txn>, Self::Error>
    where
        'env: 'txn,
    {
        let mut db = self.db.write();
        let db = db.entry(name.to_owned()).or_insert_with(Db::new);
        Ok(db.clone())
    }
}

impl<'env> Transaction<'env, EchoDbEngine> for Tx<Key, Val> {
    type Error = echodb::Error;

    fn upgrade(
        &mut self,
    ) -> Result<Option<&mut <EchoDbEngine as StorageEngine>::WriteTransaction<'env>>, Self::Error>
    {
        todo!()
    }
}

impl<'env> Transaction<'env, EchoDbEngine> for Arc<Tx<Key, Val>> {
    type Error = echodb::Error;

    fn upgrade(
        &mut self,
    ) -> Result<Option<&mut <EchoDbEngine as StorageEngine>::WriteTransaction<'env>>, Self::Error>
    {
        todo!()
    }
}

impl<'env> WriteTransaction<'env, EchoDbEngine> for Tx<Key, Val> {
    fn commit(self) -> Result<(), Self::Error> {
        todo!()
    }

    fn rollback(self) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<'env, 'txn> ReadTree<'env, 'txn, EchoDbEngine> for Db<Key, Val> {
    type Bytes = Vec<u8>;

    fn get(
        &'txn self,
        txn: &'txn <EchoDbEngine as StorageEngine>::Transaction<'_>,
        key: &[u8],
    ) -> Result<Option<Self::Bytes>, echodb::Error> {
        txn.get(key)
    }

    fn range(
        &'txn self,
        txn: &'txn <EchoDbEngine as StorageEngine>::Transaction<'_>,
        range: impl std::ops::RangeBounds<[u8]>,
    ) -> Result<
        impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), echodb::Error>>,
        echodb::Error,
    > {
        Ok(txn.scan(range)?.map(|(k, v)| (k.clone(), v.clone())).map(Ok))
    }

    fn rev_range(
        &'txn self,
        txn: &'txn <EchoDbEngine as StorageEngine>::Transaction<'_>,
        range: impl std::ops::RangeBounds<[u8]>,
    ) -> Result<
        impl Iterator<Item = Result<(Self::Bytes, Self::Bytes), <EchoDbEngine as StorageEngine>::Error>>,
        <EchoDbEngine as StorageEngine>::Error,
    > {
        Ok(txn.scan(range)?.rev().map(|(k, v)| (k.clone(), v.clone())).map(Ok))
    }
}

impl<'env, 'txn> WriteTree<'env, 'txn, EchoDbEngine> for Db<Key, Val> {
    fn put(
        &mut self,
        txn: &mut <EchoDbEngine as StorageEngine>::WriteTransaction<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), <EchoDbEngine as StorageEngine>::Error> {
        txn.put(key.to_vec(), value.to_vec())?;
        Ok(())
    }

    fn delete(
        &mut self,
        txn: &mut <EchoDbEngine as StorageEngine>::WriteTransaction<'_>,
        key: &[u8],
    ) -> Result<bool, <EchoDbEngine as StorageEngine>::Error> {
        txn.del(key)?;
        Ok(true)
    }
}
