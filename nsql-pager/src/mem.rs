use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::os::unix::prelude::FileExt;

use crate::Storage;

#[derive(Default)]
pub(crate) struct InMemory {
    buf: RefCell<Vec<u8>>,
}

impl FileExt for InMemory {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (&self.buf.borrow()[offset as usize..]).read(buf)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let offset = offset as usize;
        let mut v = self.buf.borrow_mut();
        v.resize(offset + buf.len(), 0);
        (&mut v[offset..]).write(buf)
    }
}

impl Storage for InMemory {}

impl InMemory {
    pub fn new() -> Self {
        Self { buf: Default::default() }
    }
}

#[cfg(test)]
mod tests;
