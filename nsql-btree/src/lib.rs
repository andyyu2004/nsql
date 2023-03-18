#![feature(async_fn_in_trait)]
#![feature(generic_const_exprs)]
#![feature(is_sorted)]
#![feature(split_array)]
#![feature(pointer_is_aligned)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod btree;
mod node;
mod page;
mod rkyv;

pub use btree::BTree;
pub use nsql_pager::Result;

use self::rkyv::Rkyv;

#[cfg(test)]
mod tests;
