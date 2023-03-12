#![feature(async_fn_in_trait)]
#![feature(generic_const_exprs)]
#![feature(trivial_bounds)]
#![feature(split_array)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod btree;
mod node;
mod page;

pub use btree::BTree;
pub use nsql_pager::Result;

#[cfg(test)]
mod tests;
