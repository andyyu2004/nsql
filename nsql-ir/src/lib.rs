#![deny(rust_2018_idioms)]

pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
}
