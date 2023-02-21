#![deny(rust_2018_idioms)]

use nsql_catalog::{Catalog, CatalogEntity, Schema};
use nsql_ir as ir;
use nsql_parse::ast::{self, HiveDistributionStyle};
use nsql_transaction::Transaction;
use smol_str::SmolStr;
use thiserror::Error;

pub struct Binder<'a> {
    catalog: &'a Catalog,
    tx: &'a Transaction,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("unimplemented: {0}")]
    Unimplemented(&'static str),
}

macro_rules! ensure {
    ($cond:expr) => {
        if !$cond {
            return Err($crate::Error::Unimplemented(stringify!($cond)).into());
        }
    };
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog, tx: &'a Transaction) -> Self {
        Self { catalog, tx }
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
                let name = self.bind_name(name)?;
                let columns = todo!();
                Ok(ir::Statement::CreateTable { name, columns })
            }
            _ => return Err(Error::Unimplemented("")),
        }
    }

    fn bind_name<T: CatalogEntity>(&self, name: &ast::ObjectName) -> Result<ir::Oid<T>> {
        let ident = self.lower_name(name)?;
        match ident {
            Ident::Qualified { schema, name } => {
                self.catalog.find::<Schema>(self.tx, &schema);
                todo!()
            }
            Ident::Unqualified { name } => todo!(),
        }
    }

    fn lower_name(&self, name: &ast::ObjectName) -> Result<Ident> {
        // FIXME naive impl for now
        match &name.0[..] {
            [] => unreachable!("empty name?"),
            [name] => Ok(Ident::Unqualified { name: name.value.as_str().into() }),
            [schema, name] => Ok(Ident::Qualified {
                schema: schema.value.as_str().into(),
                name: name.value.as_str().into(),
            }),
            [_, _, ..] => Err(Error::Unimplemented("x.y.z name"))?,
        }
    }
}

enum Ident {
    Qualified { schema: SmolStr, name: SmolStr },
    Unqualified { name: SmolStr },
}
