#![allow(dead_code)] // for now

use std::num::NonZeroU64;
use std::sync::atomic::{self, AtomicU64};
use std::{fmt, io};

use btree::PageIdx;
use memmap2::MmapMut;
use parking_lot::{Condvar, Mutex};

mod btree;

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = io::Error;

pub struct Env {
    mmap: MmapMut,
    root_page: PageIdx,
    page_size: usize,
    next_txn_id: AtomicU64,
    live_write_txn: Mutex<Option<TransactionId>>,
    write_txn_available: Condvar,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionId(NonZeroU64);

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Env {
    pub fn open() -> Result<Self> {
        // Just allocate a large chunk of memory and never worry about resizing.
        // TODO experiment with HUGE_TLB to see what that does
        let mmap = MmapMut::map_anon(u32::MAX as usize)?;
        const PAGE_SIZE: usize = 4096;
        Ok(Self {
            mmap,
            root_page: PageIdx::INVALID,
            page_size: PAGE_SIZE,
            next_txn_id: AtomicU64::new(1),
            live_write_txn: Default::default(),
            write_txn_available: Default::default(),
        })
    }

    pub fn begin(&self) -> ReadTransaction<'_> {
        ReadTransaction { env: self }
    }

    pub fn begin_write(&self) -> WriteTransaction<'_> {
        let mut live_write_txn = self.live_write_txn.lock();
        while live_write_txn.is_some() {
            self.write_txn_available.wait(&mut live_write_txn);
        }
        assert!(live_write_txn.is_none());

        WriteTransaction { id: self.next_txn_id(), env: self }
    }

    fn next_txn_id(&self) -> TransactionId {
        TransactionId(
            NonZeroU64::new(self.next_txn_id.fetch_add(1, atomic::Ordering::AcqRel)).unwrap(),
        )
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

    pub fn insert(&self, key: &[u8], _value: &[u8]) {
        let page = self.env.search(key);
        todo!()
    }
}
