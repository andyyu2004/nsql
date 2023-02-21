#![deny(rust_2018_idioms)]

use nsql_catalog::{Name, Oid, Schema};

#[derive(Debug)]
pub enum Statement {
    CreateTable { schema: Oid<Schema>, name: Name, columns: Vec<ColumnDef> },
}

#[derive(Debug)]
pub struct ColumnDef {
    pub name: Name,
    pub ty: Ty,
}

#[derive(Debug)]
pub enum Ty {
    Int,
}
