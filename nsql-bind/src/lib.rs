#![deny(rust_2018_idioms)]

mod scope;

use std::fmt;
use std::str::FromStr;

use ir::Decimal;
use nsql_catalog::{
    Catalog, Column, Container, CreateColumnInfo, Entity, Namespace, NamespaceEntity, Oid, Table,
    DEFAULT_SCHEMA,
};
use nsql_core::schema::LogicalType;
use nsql_core::Name;
use nsql_parse::ast::{self, HiveDistributionStyle};
use nsql_transaction::Transaction;
use thiserror::Error;

use self::scope::Scope;

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

    #[error("unbound {kind} `{ident}`")]
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
    pub fn new(tx: &'a Transaction, catalog: &'a Catalog) -> Self {
        Self { catalog, tx }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<ir::Stmt> {
        let init_scope = Scope::default();
        self.bind_stmt(&init_scope, stmt)
    }

    fn bind_stmt(&self, scope: &Scope, stmt: &ast::Statement) -> Result<ir::Stmt> {
        let stmt = match stmt {
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

                let ident = self.lower_ident(&name.0)?;
                match self.bind_ident::<Table>(&ident) {
                    Ok(_oid) => {
                        return Err(Error::AlreadyExists {
                            kind: Table::desc(),
                            ident: ident.clone(),
                        });
                    }
                    Err(_) => {
                        let namespace = self.bind_namespace(&ident)?;
                        let columns = self.lower_columns(columns)?;
                        let info = ir::CreateTableInfo { name: ident.name(), columns };
                        ir::Stmt::CreateTable { namespace, info }
                    }
                }
            }
            ast::Statement::Insert {
                or,
                into: _,
                table_name,
                columns: _,
                overwrite,
                source,
                partitioned,
                after_columns,
                table: _,
                on,
                returning,
            } => {
                ensure!(or.is_none());
                ensure!(!overwrite);
                ensure!(partitioned.is_none());
                ensure!(after_columns.is_empty());
                ensure!(on.is_none());
                ensure!(returning.is_none());
                // ensure!(columns.is_empty());

                let table_name = self.lower_ident(&table_name.0)?;
                let (namespace, table) = self.bind_namespaced_entity::<Table>(&table_name)?;
                let source = self.bind_query(scope, source)?;
                let returning = returning.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|selection| self.bind_select_item(scope, selection))
                        .collect::<Result<Vec<_>>>()
                        .unwrap()
                });
                ir::Stmt::Insert { namespace, table, source, returning }
            }
            ast::Statement::Query(query) => ir::Stmt::Query(self.bind_query(scope, query)?),
            _ => return Err(Error::Unimplemented("unimplemented stmt".into())),
        };

        Ok(stmt)
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

    fn lower_ty(&self, ty: &ast::DataType) -> Result<LogicalType> {
        match ty {
            ast::DataType::Int(width) if width.is_none() => Ok(LogicalType::Int),
            ty => Err(Error::Unimplemented(format!("type {ty:?}")))?,
        }
    }

    fn bind_namespace(&self, ident: &Ident) -> Result<Oid<Namespace>> {
        match ident {
            Ident::Qualified { schema, .. } => self
                .catalog
                .find::<Namespace>(schema.as_str())?
                .ok_or_else(|| Error::Unbound { kind: Namespace::desc(), ident: ident.clone() }),
            Ident::Unqualified { name } => self.bind_namespace(&Ident::Qualified {
                schema: DEFAULT_SCHEMA.into(),
                name: name.clone(),
            }),
        }
    }

    fn bind_ident<T: NamespaceEntity>(&self, ident: &Ident) -> Result<(Oid<Namespace>, Oid<T>)> {
        match ident {
            Ident::Qualified { schema, name } => {
                let (schema_oid, schema) =
                    self.catalog.get_by_name::<Namespace>(self.tx, schema.as_str())?.ok_or_else(
                        || Error::Unbound { kind: Namespace::desc(), ident: ident.clone() },
                    )?;

                let entity_oid = schema
                    .find(name)?
                    .ok_or_else(|| Error::Unbound { kind: T::desc(), ident: ident.clone() })?;

                Ok((schema_oid, entity_oid))
            }
            Ident::Unqualified { name } => self.bind_ident(&Ident::Qualified {
                schema: DEFAULT_SCHEMA.into(),
                name: name.clone(),
            }),
        }
    }

    fn bind_namespaced_entity<T: NamespaceEntity>(
        &self,
        name: &Ident,
    ) -> Result<(Oid<Namespace>, Oid<T>)> {
        self.bind_ident(name)
    }

    fn lower_ident(&self, name: &[ast::Ident]) -> Result<Ident> {
        // FIXME naive impl for now
        match name {
            [] => unreachable!("empty name?"),
            [name] => Ok(Ident::Unqualified { name: name.value.as_str().into() }),
            [schema, name] => Ok(Ident::Qualified {
                schema: schema.value.as_str().into(),
                name: name.value.as_str().into(),
            }),
            [_, _, ..] => Err(Error::Unimplemented("x.y.z name".into()))?,
        }
    }

    fn bind_query(&self, scope: &Scope, query: &ast::Query) -> Result<ir::TableExpr> {
        let ast::Query { with, body, order_by, limit, offset, fetch, locks } = query;
        ensure!(with.is_none());
        ensure!(order_by.is_empty());
        ensure!(limit.is_none());
        ensure!(offset.is_none());
        ensure!(fetch.is_none());
        ensure!(locks.is_empty());

        self.bind_table_expr(scope, body)
    }

    fn bind_table_expr(&self, scope: &Scope, body: &ast::SetExpr) -> Result<ir::TableExpr> {
        let expr = match body {
            ast::SetExpr::Select(sel) => ir::TableExpr::Selection(self.bind_select(scope, sel)?),
            ast::SetExpr::Query(_) => todo!(),
            ast::SetExpr::SetOperation { .. } => todo!(),
            ast::SetExpr::Values(values) => ir::TableExpr::Values(self.bind_values(scope, values)?),
            ast::SetExpr::Insert(_) => todo!(),
            ast::SetExpr::Table(_) => todo!(),
        };

        Ok(expr)
    }

    fn bind_select(&self, scope: &Scope, select: &ast::Select) -> Result<Vec<ir::Expr>> {
        let ast::Select {
            distinct,
            projection,
            top,
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            qualify,
        } = select;
        ensure!(!distinct);
        ensure!(top.is_none());
        ensure!(into.is_none());
        ensure!(lateral_views.is_empty());
        ensure!(selection.is_none());
        ensure!(group_by.is_empty());
        ensure!(cluster_by.is_empty());
        ensure!(distribute_by.is_empty());
        ensure!(sort_by.is_empty());
        ensure!(having.is_none());
        ensure!(qualify.is_none());

        let scope = match &from[..] {
            [] => scope.clone(),
            [table] => self.bind_tables(scope, table)?,
            _ => todo!(),
        };

        projection.iter().map(|item| self.bind_select_item(&scope, item)).collect()
    }

    fn bind_tables(&self, scope: &Scope, tables: &ast::TableWithJoins) -> Result<Scope> {
        ensure!(tables.joins.is_empty());
        let table = &tables.relation;
        match table {
            ast::TableFactor::Table { name, alias, args, with_hints } => {
                ensure!(args.is_none());
                ensure!(with_hints.is_empty());
                ensure!(alias.is_none());

                let (name, (namespace_oid, table_oid)) = self.bind_table(name)?;
                let namespace = self.catalog.get(self.tx, namespace_oid)?.unwrap();
                let table = namespace.get(self.tx, table_oid)?.unwrap();
                let columns = table.all::<Column>(self.tx)?;
                Ok(scope.bind_table(name, (namespace_oid, table_oid), columns))
            }
            ast::TableFactor::Derived { .. } => todo!(),
            ast::TableFactor::TableFunction { .. } => todo!(),
            ast::TableFactor::UNNEST { .. } => todo!(),
            ast::TableFactor::NestedJoin { .. } => todo!(),
        }
    }

    fn bind_table(
        &self,
        table_name: &ast::ObjectName,
    ) -> Result<(Ident, (Oid<Namespace>, Oid<Table>))> {
        let table_name = self.lower_ident(&table_name.0)?;
        let bound = self.bind_namespaced_entity::<Table>(&table_name)?;
        Ok((table_name, bound))
    }

    fn bind_select_item(&self, scope: &Scope, item: &ast::SelectItem) -> Result<ir::Expr> {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_expr(scope, expr),
            _ => todo!(),
        }
    }

    fn bind_values(&self, scope: &Scope, values: &ast::Values) -> Result<ir::Values> {
        values.rows.iter().map(|row| self.bind_row(scope, row)).collect()
    }

    fn bind_row(&self, scope: &Scope, row: &[ast::Expr]) -> Result<Vec<ir::Expr>> {
        row.iter().map(|expr| self.bind_expr(scope, expr)).collect()
    }

    fn bind_expr(&self, scope: &Scope, expr: &ast::Expr) -> Result<ir::Expr> {
        let expr = match expr {
            ast::Expr::Value(literal) => ir::Expr::Literal(self.bind_literal(literal)),
            ast::Expr::Identifier(ident) => ir::Expr::ColumnRef(
                scope.lookup_column(&Ident::Unqualified { name: ident.value.clone().into() })?,
            ),
            ast::Expr::CompoundIdentifier(ident) => {
                let ident = self.lower_ident(ident)?;
                ir::Expr::ColumnRef(scope.lookup_column(&ident)?)
            }
            _ => todo!(),
        };
        Ok(expr)
    }

    fn bind_literal(&self, literal: &ast::Value) -> ir::Literal {
        match literal {
            ast::Value::Number(decimal, b) => {
                assert!(!b, "what does this bool mean?");
                let decimal = Decimal::from_str(decimal)
                    .expect("this should be a parse error if the decimal is not valid");
                ir::Literal::Decimal(decimal)
            }
            ast::Value::SingleQuotedString(_) => todo!(),
            ast::Value::DollarQuotedString(_) => todo!(),
            ast::Value::EscapedStringLiteral(_) => todo!(),
            ast::Value::NationalStringLiteral(_) => todo!(),
            ast::Value::HexStringLiteral(_) => todo!(),
            ast::Value::DoubleQuotedString(_) => todo!(),
            ast::Value::Boolean(b) => ir::Literal::Bool(*b),
            ast::Value::Null => ir::Literal::Null,
            ast::Value::Placeholder(_) => todo!(),
            ast::Value::UnQuotedString(_) => todo!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Ident {
    Qualified { schema: Name, name: Name },
    Unqualified { name: Name },
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
