#![deny(rust_2018_idioms)]

use std::fmt;

use nsql_catalog::{
    Catalog, Container, CreateColumnInfo, CreateTableInfo, Entity, Name, Oid, Schema, SchemaEntity,
    Table, Ty, DEFAULT_SCHEMA,
};
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
    Unimplemented(String),

    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),

    #[error("unbound {kind} in `{ident}`")]
    Unbound { kind: &'static str, ident: Ident },

    #[error("{kind} already exists: `{ident}`")]
    AlreadyExists { kind: &'static str, ident: Ident },
}

macro_rules! ensure {
    ($cond:expr) => {
        if !$cond {
            return Err($crate::Error::Unimplemented(stringify!($cond).into()).into());
        }
    };
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog, tx: &'a Transaction) -> Self {
        Self { catalog, tx }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<ir::Stmt> {
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
                hive_formats: _,
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

                let ident = self.lower_name(name)?;
                match self.bind_ident::<Table>(&ident) {
                    Ok(_oid) => {
                        Err(Error::AlreadyExists { kind: Table::desc(), ident: ident.clone() })
                    }
                    Err(_) => {
                        let schema = self.bind_schema(&ident)?;
                        let columns = self.lower_columns(columns)?;
                        let info = CreateTableInfo { name: ident.name(), columns };
                        Ok(ir::Stmt::CreateTable { schema, info })
                    }
                }
            }
            _ => Err(Error::Unimplemented("unimplemented stmt".into())),
        }
    }

    fn lower_columns(&self, columns: &[ast::ColumnDef]) -> Result<Vec<CreateColumnInfo>> {
        columns.iter().map(|c| self.lower_column(c)).collect()
    }

    fn lower_column(&self, column: &ast::ColumnDef) -> Result<CreateColumnInfo> {
        Ok(CreateColumnInfo {
            name: column.name.value.as_str().into(),
            ty: self.lower_ty(&column.data_type)?,
        })
    }

    fn lower_ty(&self, ty: &ast::DataType) -> Result<Ty> {
        match ty {
            ast::DataType::Int(width) if width.is_none() => Ok(Ty::Int),
            ty => Err(Error::Unimplemented(format!("type {ty:?}")))?,
        }
    }

    fn bind_schema(&self, ident: &Ident) -> Result<Oid<Schema>> {
        match ident {
            Ident::Qualified { schema, .. } => self
                .catalog
                .find::<Schema>(schema.as_str())?
                .ok_or_else(|| Error::Unbound { kind: Schema::desc(), ident: ident.clone() }),
            Ident::Unqualified { name } => self.bind_schema(&Ident::Qualified {
                schema: DEFAULT_SCHEMA.into(),
                name: name.clone(),
            }),
        }
    }

    fn bind_ident<T: SchemaEntity>(&self, ident: &Ident) -> Result<Oid<T>> {
        match ident {
            Ident::Qualified { schema, name } => {
                let schema = self
                    .catalog
                    .get_by_name::<Schema>(self.tx, schema.as_str())?
                    .ok_or_else(|| Error::Unbound { kind: Schema::desc(), ident: ident.clone() })?;

                schema
                    .find(name)?
                    .ok_or_else(|| Error::Unbound { kind: T::desc(), ident: ident.clone() })
            }
            Ident::Unqualified { name } => self.bind_ident(&Ident::Qualified {
                schema: DEFAULT_SCHEMA.into(),
                name: name.clone(),
            }),
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
            [_, _, ..] => Err(Error::Unimplemented("x.y.z name".into()))?,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Ident {
    Qualified { schema: SmolStr, name: SmolStr },
    Unqualified { name: SmolStr },
}

impl Ident {
    pub fn schema(&self) -> Option<Name> {
        match self {
            Ident::Qualified { schema, .. } => Some(schema.as_str().into()),
            Ident::Unqualified { .. } => None,
        }
    }

    pub fn name(&self) -> Name {
        match self {
            Ident::Qualified { name, .. } => name.as_str().into(),
            Ident::Unqualified { name } => name.as_str().into(),
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Ident::Qualified { schema, name: object } => write!(f, "{schema}.{object}"),
            Ident::Unqualified { name } => write!(f, "{name}"),
        }
    }
}
