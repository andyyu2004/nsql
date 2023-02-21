#![deny(rust_2018_idioms)]

use std::marker::PhantomData;

use nsql_catalog::{CatalogEntity, Table};

pub enum Statement {
    CreateTable { name: Oid<Table>, columns: Vec<ColumnDef> },
}

pub struct Oid<T: CatalogEntity> {
    phantom: PhantomData<fn() -> T>,
}

pub struct ColumnDef {}
