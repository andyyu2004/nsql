#![allow(dead_code)] // for now

use std::num::NonZeroU64;
use std::{fmt, io};

use memmap2::MmapMut;

mod btree;

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = io::Error;

pub struct Env {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionId(NonZeroU64);

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Env {
    pub fn open() -> Result<Self> {
        Ok(Self {})
    }
}

pub struct ReadTransaction<'env> {
    env: &'env Env,
}

impl<'env> ReadTransaction<'env> {
    pub fn open(&self, _keyspace: &str) -> Result<ReadTable<'env, '_>> {
        Ok(ReadTable { env: self.env, txn: self })
    }
}

pub struct WriteTransaction<'env> {
    id: TransactionId,
    env: &'env Env,
}

impl<'env> WriteTransaction<'env> {
    pub fn open(&self, _keyspace: &str) -> Result<WriteTable<'env, '_>> {
        Ok(WriteTable { env: self.env, txn: self })
    }
}

pub struct ReadTable<'env, 'txn> {
    env: &'env Env,
    txn: &'txn ReadTransaction<'env>,
}

pub struct WriteTable<'env, 'txn> {
    env: &'env Env,
    txn: &'txn WriteTransaction<'env>,
}

impl<'env, 'txn> WriteTable<'env, 'txn> {
    pub fn get(&self, _key: &[u8]) -> Option<&[u8]> {
        todo!()
    }

    pub fn insert(&self, _key: &[u8], _value: &[u8]) {
        todo!()
    }
}
