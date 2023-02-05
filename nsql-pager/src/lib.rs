#![deny(rust_2018_idioms)]

mod mem;
mod storage;

use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::path::Path;

use self::mem::InMemory;
use self::storage::Storage;

pub const DEFAULT_PAGE_SIZE: usize = 4096;

pub struct Pager {
    page_size: usize,
    storage: Box<dyn Storage>,
}

pub struct Page {
    data: Vec<u8>,
}

pub enum DbPath<'a> {
    InMemory,
    Path(Cow<'a, Path>),
}

impl<'a, P> From<&'a P> for DbPath<'a>
where
    P: AsRef<Path>,
{
    fn from(path: &'a P) -> Self {
        DbPath::Path(Cow::Borrowed(path.as_ref()))
    }
}

pub struct PageIndex(usize);

impl Pager {
    pub fn open<'a>(path: impl Into<DbPath<'a>>) -> io::Result<Pager> {
        let storage = match path.into() {
            DbPath::InMemory => Box::new(InMemory::new()) as Box<dyn Storage>,
            DbPath::Path(path) => Box::new(File::open(path)?),
        };

        Ok(Pager { page_size: DEFAULT_PAGE_SIZE, storage })
    }

    pub fn fetch(&self, index: PageIndex) -> io::Result<Page> {
        let offset = index.0 * self.page_size;
        let mut data = Vec::with_capacity(self.page_size);
        self.storage.read_exact_at(&mut data, offset as u64)?;
        Ok(Page { data })
    }
}
