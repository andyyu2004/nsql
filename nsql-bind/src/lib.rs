#![deny(rust_2018_idioms)]

pub use anyhow::Error;
use anyhow::{bail, ensure};
use nsql_catalog::Catalog;
use nsql_ir as ir;
use nsql_parse::ast::{self, HiveDistributionStyle};

pub struct Binder<'c> {
    catalog: &'c Catalog,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<'c> Binder<'c> {
    pub fn new(catalog: &'c Catalog) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<ir::Statement> {
        match stmt {
            ast::Statement::CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                default_charset,
                collation,
                on_commit,
                on_cluster,
            } => {
                ensure!(!or_replace);
                ensure!(!temporary);
                ensure!(!external);
                ensure!(global.is_none());
                ensure!(!if_not_exists);
                ensure!(constraints.is_empty());
                ensure!(*hive_distribution == HiveDistributionStyle::NONE);
                ensure!(hive_formats.is_none());
                ensure!(table_properties.is_empty());
                ensure!(with_options.is_empty());
                ensure!(file_format.is_none());
                ensure!(location.is_none());
                ensure!(query.is_none());
                ensure!(!without_rowid);
                ensure!(like.is_none());
                ensure!(clone.is_none());
                ensure!(engine.is_none());
                ensure!(default_charset.is_none());
                ensure!(collation.is_none());
                ensure!(on_commit.is_none());
                ensure!(on_cluster.is_none());
                ir::Statement::CreateTable { name, columns }
            }
            _ => bail!("not implemented"),
        }
    }
}
