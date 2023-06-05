// Slightly patched version of echodb

// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::sync::Arc;

use arc_swap::ArcSwap;
use imbl::OrdMap;
use parking_lot::lock_api::ArcMutexGuard;
use parking_lot::{Mutex, RawMutex};

#[derive(Clone, Default)]
pub struct Db<K, V> {
    pub(crate) lk: Arc<Mutex<()>>,
    pub(crate) ds: Arc<ArcSwap<OrdMap<K, V>>>,
}

impl<K, V> Db<K, V> {
    // Open a new database
    pub fn new() -> Db<K, V> {
        Db { lk: Arc::new(Mutex::new(())), ds: Arc::new(ArcSwap::new(Arc::new(OrdMap::new()))) }
    }
}

impl<K, V> Db<K, V>
where
    K: Ord + Clone,
    V: Eq + Clone,
{
    // Start a new transaction
    pub fn begin(&self, write: bool) -> Result<Tx<K, V>, Error> {
        match write {
            true => Ok(Tx::new(self.ds.clone(), write, Some(self.lk.lock_arc()))),
            false => Ok(Tx::new(self.ds.clone(), write, None)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn begin_tx_readable() {
        let db: Db<Key, Val> = Db::new();
        let tx = db.begin(false);
        assert!(tx.is_ok());
    }

    #[test]
    fn begin_tx_writeable() {
        let db: Db<Key, Val> = Db::new();
        let tx = db.begin(true);
        assert!(tx.is_ok());
    }

    #[test]
    fn begin_tx_readable_async() {
        let db: Db<Key, Val> = Db::new();
        let tx = db.begin(false);
        assert!(tx.is_ok());
    }

    #[test]
    fn begin_tx_writeable_async() {
        let db: Db<Key, Val> = Db::new();
        let tx = db.begin(true);
        assert!(tx.is_ok());
    }

    #[test]
    fn writeable_tx_across_async() {
        let db: Db<&str, &str> = Db::new();
        let mut tx = db.begin(true).unwrap();
        let res = tx.put("test", "something");
        assert!(res.is_ok());
        let res = tx.get("test");
        assert!(res.is_ok());
        let res = tx.commit();
        assert!(res.is_ok());
    }

    #[test]
    fn readable_tx_not_writable() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(false).unwrap();
        let res = tx.put("test", "something");
        assert!(res.is_err());
        let res = tx.set("test", "something");
        assert!(res.is_err());
        let res = tx.del("test");
        assert!(res.is_err());
        let res = tx.commit();
        assert!(res.is_err());
        let res = tx.cancel();
        assert!(res.is_ok());
    }

    #[test]
    fn finished_tx_not_writeable() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(false).unwrap();
        let res = tx.cancel();
        assert!(res.is_ok());
        let res = tx.put("test", "something");
        assert!(res.is_err());
        let res = tx.set("test", "something");
        assert!(res.is_err());
        let res = tx.del("test");
        assert!(res.is_err());
        let res = tx.commit();
        assert!(res.is_err());
        let res = tx.cancel();
        assert!(res.is_err());
    }

    #[test]
    fn cancelled_tx_is_cancelled() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(true).unwrap();
        tx.put("test", "something").unwrap();
        let res = tx.exists("test").unwrap();
        assert!(res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, Some("something"));
        let res = tx.cancel();
        assert!(res.is_ok());
        // ----------
        let mut tx = db.begin(false).unwrap();
        let res = tx.exists("test").unwrap();
        assert!(!res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, None);
        let res = tx.cancel();
        assert!(res.is_ok());
    }

    #[test]
    fn committed_tx_is_committed() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(true).unwrap();
        tx.put("test", "something").unwrap();
        let res = tx.exists("test").unwrap();
        assert!(res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, Some("something"));
        let res = tx.commit();
        assert!(res.is_ok());
        // ----------
        let mut tx = db.begin(false).unwrap();
        let res = tx.exists("test").unwrap();
        assert!(res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, Some("something"));
        let res = tx.cancel();
        assert!(res.is_ok());
    }

    #[test]
    fn multiple_concurrent_readers() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(true).unwrap();
        tx.put("test", "something").unwrap();
        let res = tx.exists("test").unwrap();
        assert!(res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, Some("something"));
        let res = tx.commit();
        assert!(res.is_ok());
        // ----------
        let mut tx1 = db.begin(false).unwrap();
        let res = tx1.exists("test").unwrap();
        assert!(res);
        let res = tx1.exists("temp").unwrap();
        assert!(!res);
        // ----------
        let mut tx2 = db.begin(false).unwrap();
        let res = tx2.exists("test").unwrap();
        assert!(res);
        let res = tx2.exists("temp").unwrap();
        assert!(!res);
        // ----------
        let res = tx1.cancel();
        assert!(res.is_ok());
        let res = tx2.cancel();
        assert!(res.is_ok());
    }

    #[test]
    fn multiple_concurrent_operators() {
        let db: Db<&str, &str> = Db::new();
        // ----------
        let mut tx = db.begin(true).unwrap();
        tx.put("test", "something").unwrap();
        let res = tx.exists("test").unwrap();
        assert!(res);
        let res = tx.get("test").unwrap();
        assert_eq!(res, Some("something"));
        let res = tx.commit();
        assert!(res.is_ok());
        // ----------
        let mut tx1 = db.begin(false).unwrap();
        let res = tx1.exists("test").unwrap();
        assert!(res);
        let res = tx1.exists("temp").unwrap();
        assert!(!res);
        // ----------
        let mut txw = db.begin(true).unwrap();
        txw.put("temp", "other").unwrap();
        let res = txw.exists("test").unwrap();
        assert!(res);
        let res = txw.exists("temp").unwrap();
        assert!(res);
        let res = txw.commit();
        assert!(res.is_ok());
        // ----------
        let mut tx2 = db.begin(false).unwrap();
        let res = tx2.exists("test").unwrap();
        assert!(res);
        let res = tx2.exists("temp").unwrap();
        assert!(res);
        // ----------
        let res = tx1.exists("temp").unwrap();
        assert!(!res);
        // ----------
        let res = tx1.cancel();
        assert!(res.is_ok());
        let res = tx2.cancel();
        assert!(res.is_ok());
    }
}
// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Transaction is closed")]
    TxClosed,

    #[error("Transaction is not writable")]
    TxNotWritable,

    #[error("Key being inserted already exists")]
    KeyAlreadyExists,

    #[error("Value being checked was not correct")]
    ValNotExpectedValue,
}

pub type Key = Vec<u8>;

pub type Val = Vec<u8>;

pub struct Kv {
    pub key: Key,
    pub val: Val,
}

// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub struct Tx<K, V> {
    // Is the transaction complete?
    pub(crate) ok: bool,
    // Is the transaction read+write?
    pub(crate) rw: bool,
    // The immutable copy of the data map
    pub(crate) ds: OrdMap<K, V>,
    // The pointer to the latest data map
    pub(crate) pt: Arc<ArcSwap<OrdMap<K, V>>>,
    // The underlying database write mutex
    pub(crate) lk: Option<ArcMutexGuard<RawMutex, ()>>,
}

impl<K, V> Tx<K, V>
where
    K: Ord + Clone,
    V: Eq + Clone,
{
    // Create a transaction
    pub(crate) fn new(
        pt: Arc<ArcSwap<OrdMap<K, V>>>,
        write: bool,
        guard: Option<ArcMutexGuard<RawMutex, ()>>,
    ) -> Tx<K, V> {
        Tx { ok: false, rw: write, lk: guard, pt: pt.clone(), ds: (*(*pt.load())).clone() }
    }
    // Check if closed
    pub fn closed(&self) -> bool {
        self.ok
    }
    // Cancel a transaction
    pub fn cancel(&mut self) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Mark this transaction as done
        self.ok = true;
        // Unlock the database mutex
        if let Some(lk) = self.lk.take() {
            drop(lk);
        }
        // Continue
        Ok(())
    }
    // Commit a transaction
    pub fn commit(&mut self) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Mark this transaction as done
        self.ok = true;
        // Commit the data
        self.pt.store(Arc::new(self.ds.clone()));
        // Unlock the database mutex
        if let Some(lk) = self.lk.take() {
            drop(lk);
        }
        // Continue
        Ok(())
    }

    // Check if a key exists
    pub fn exists(&self, key: K) -> Result<bool, Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check the key
        let res = self.ds.contains_key(&key);
        // Return result
        Ok(res)
    }

    // Fetch a key from the database
    pub fn get<BK>(&self, key: &BK) -> Result<Option<V>, Error>
    where
        BK: Ord + ?Sized,
        K: Borrow<BK>,
    {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Get the key
        let res = self.ds.get(key).cloned();
        // Return result
        Ok(res)
    }
    // Insert or update a key in the database
    pub fn set(&mut self, key: K, val: V) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Set the key
        self.ds.insert(key, val);
        // Return result
        Ok(())
    }
    // Insert a key if it doesn't exist in the database
    pub fn put(&mut self, key: K, val: V) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Set the key
        match self.ds.get(&key) {
            None => self.ds.insert(key, val),
            _ => return Err(Error::KeyAlreadyExists),
        };
        // Return result
        Ok(())
    }
    // Insert a key if it matches a value
    pub fn putc(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Set the key
        match (self.ds.get(&key), &chk) {
            (Some(v), Some(w)) if v == w => self.ds.insert(key, val),
            (None, None) => self.ds.insert(key, val),
            _ => return Err(Error::ValNotExpectedValue),
        };
        // Return result
        Ok(())
    }
    // Delete a key
    pub fn del<BK>(&mut self, key: &BK) -> Result<(), Error>
    where
        BK: Ord + ?Sized,
        K: Borrow<BK>,
    {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Remove the key
        self.ds.remove(key);
        // Return result
        Ok(())
    }
    // Delete a key if it matches a value
    pub fn delc(&mut self, key: K, chk: Option<V>) -> Result<(), Error> {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Check to see if transaction is writable
        if !self.rw {
            return Err(Error::TxNotWritable);
        }
        // Remove the key
        match (self.ds.get(&key), &chk) {
            (Some(v), Some(w)) if v == w => self.ds.remove(&key),
            (None, None) => self.ds.remove(&key),
            _ => return Err(Error::ValNotExpectedValue),
        };
        // Return result
        Ok(())
    }
    // Retrieve a range of keys from the databases
    pub fn scan<KR>(
        &self,
        rng: impl RangeBounds<KR>,
    ) -> Result<impl DoubleEndedIterator<Item = (&K, &V)>, Error>
    where
        K: Borrow<KR>,
        KR: Ord + ?Sized,
    {
        // Check to see if transaction is closed
        if self.ok {
            return Err(Error::TxClosed);
        }
        // Scan the keys
        Ok(self.ds.range(rng))
    }
}
