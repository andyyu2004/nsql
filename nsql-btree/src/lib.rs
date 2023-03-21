#![feature(min_specialization, rustc_attrs)]
#![feature(async_fn_in_trait)]
#![feature(generic_const_exprs)]
#![feature(is_sorted)]
#![feature(split_array)]
#![feature(pointer_is_aligned)]
#![allow(incomplete_features)]
#![deny(rust_2018_idioms)]

mod btree;
mod page;
mod rkyv;

pub use btree::{BTree, Min};
pub use nsql_pager::Result;

#[cfg(test)]
mod tests;
