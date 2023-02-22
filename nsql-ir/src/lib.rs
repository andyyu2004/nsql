#![deny(rust_2018_idioms)]

use nsql_catalog::{CreateTableInfo, Oid, Schema};

#[derive(Debug, Clone)]
pub enum Stmt {
    CreateTable { schema: Oid<Schema>, info: CreateTableInfo },
}
