#![deny(rust_2018_idioms)]

use std::io;

use nsql_pager::{DbPath, Pager};

pub struct BTree {
    pager: Pager,
}

impl BTree {
    pub fn open<'a>(path: impl Into<DbPath<'a>>) -> io::Result<Self> {
        let pager = Pager::open(path)?;
        Ok(Self { pager })
    }
}
