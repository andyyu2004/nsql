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

    #[error("{0}")]
    Generic(String),

    #[error(transparent)]
    Catalog(#[from] nsql_catalog::Error),

    #[error("unbound {kind} `{ident}`")]
    Unbound { kind: &'static str, ident: Ident },
}

macro_rules! not_implemented {
    ($msg:literal) => {
        return Err($crate::Error::Unimplemented($msg.into()).into())
    };
    ($cond:expr) => {
        if $cond {
            return Err($crate::Error::Unimplemented(stringify!($cond).into()).into());
        }
    };
}

macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err($crate::Error::Generic($msg.into()));
        }
    };
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<'a> Binder<'a> {
    pub fn new(tx: &'a Transaction, catalog: &'a Catalog) -> Self {
        Self { catalog, tx }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<ir::Stmt> {
        let scope = &Scope::default();
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
                transient,
                order_by,
            } => {
                not_implemented!(*or_replace);
                not_implemented!(*temporary);
                not_implemented!(*external);
                not_implemented!(global.is_some());
                not_implemented!(*if_not_exists);
                not_implemented!(!constraints.is_empty());
                not_implemented!(*hive_distribution != HiveDistributionStyle::NONE);
                not_implemented!(!table_properties.is_empty());
                not_implemented!(!with_options.is_empty());
                not_implemented!(file_format.is_some());
                not_implemented!(location.is_some());
                not_implemented!(query.is_some());
                not_implemented!(*without_rowid);
                not_implemented!(like.is_some());
                not_implemented!(clone.is_some());
                not_implemented!(engine.is_some());
                not_implemented!(default_charset.is_some());
                not_implemented!(collation.is_some());
                not_implemented!(on_commit.is_some());
                not_implemented!(on_cluster.is_some());
                not_implemented!(*transient);
                not_implemented!(order_by.is_some());

                let ident = self.lower_ident(&name.0)?;
                let namespace = self.bind_namespace(&ident)?;
                let columns = self.lower_columns(columns)?;
                let info = ir::CreateTableInfo { name: ident.name(), namespace, columns };
                ir::Stmt::CreateTable(info)
            }
            ast::Statement::CreateSchema { schema_name, if_not_exists } => {
                not_implemented!(*if_not_exists);
                let name = match schema_name {
                    ast::SchemaName::Simple(ident) => match self.lower_ident(&ident.0)? {
                        Ident::Qualified { .. } => {
                            todo!("what does it mean for a schema to be qualified?")
                        }
                        Ident::Unqualified { name } => name,
                    },
                    ast::SchemaName::UnnamedAuthorization(_)
                    | ast::SchemaName::NamedAuthorization(_, _) => {
                        not_implemented!("schema name with authorization")
                    }
                };
                ir::Stmt::CreateNamespace(ir::CreateNamespaceInfo {
                    name,
                    if_not_exists: *if_not_exists,
                })
            }
            ast::Statement::Insert {
                or,
                into: _,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table: _,
                on,
                returning,
            } => {
                not_implemented!(or.is_some());
                not_implemented!(*overwrite);
                not_implemented!(partitioned.is_some());
                not_implemented!(!after_columns.is_empty());
                not_implemented!(on.is_some());

                // We bind the columns of the table first, so that we can use them in the following projection
                let (scope, table_ref) = self.bind_table(scope, table_name)?;

                // We model the insertion columns list as a projection over the source
                let projection = columns
                    .iter()
                    .map(|ident| ast::Expr::Identifier(ident.clone()))
                    .map(|expr| self.bind_expr(&scope, &expr))
                    .collect::<Result<Vec<_>>>()?;

                let (scope, source) = self.bind_query(&scope, source)?;
                let returning = returning.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|selection| self.bind_select_item(&scope, selection))
                        .collect::<Result<Vec<_>>>()
                        .unwrap()
                });
                ir::Stmt::Insert { table_ref, projection, source, returning }
            }
            ast::Statement::Query(query) => {
                let (_scope, query) = self.bind_query(scope, query)?;
                ir::Stmt::Query(query)
            }
            ast::Statement::StartTransaction { modes } => {
                not_implemented!(!modes.is_empty());
                ir::Stmt::Transaction(ir::TransactionKind::Begin)
            }
            ast::Statement::Rollback { chain } => {
                not_implemented!(*chain);
                ir::Stmt::Transaction(ir::TransactionKind::Rollback)
            }
            ast::Statement::Commit { chain } => {
                not_implemented!(*chain);
                ir::Stmt::Transaction(ir::TransactionKind::Commit)
            }
            ast::Statement::ShowTables { extended, full, db_name, filter } => {
                not_implemented!(*extended);
                not_implemented!(*full);
                not_implemented!(db_name.is_some());
                not_implemented!(filter.is_some());
                ir::Stmt::Show(ir::ObjectType::Table)
            }
            ast::Statement::Drop { object_type, if_exists, names, cascade, restrict, purge } => {
                not_implemented!(*if_exists);
                not_implemented!(*cascade);
                not_implemented!(*restrict);
                not_implemented!(*purge);

                let names = names
                    .iter()
                    .map(|name| self.lower_ident(&name.0))
                    .collect::<Result<Vec<_>>>()?;

                let refs = names
                    .iter()
                    .map(|name| match object_type {
                        ast::ObjectType::Table => {
                            let (namespace, table) = self.bind_namespaced_entity::<Table>(name)?;
                            Ok(ir::EntityRef::Table(ir::TableRef { namespace, table }))
                        }
                        ast::ObjectType::View => todo!(),
                        ast::ObjectType::Index => todo!(),
                        ast::ObjectType::Schema => todo!(),
                        ast::ObjectType::Role => not_implemented!("roles"),
                        ast::ObjectType::Sequence => todo!(),
                        ast::ObjectType::Stage => not_implemented!("stages"),
                    })
                    .collect::<Result<Vec<_>, Error>>()?;

                ir::Stmt::Drop(refs)
            }
            _ => return Err(Error::Unimplemented("unimplemented stmt".into())),
        };

        Ok(stmt)
    }

    fn lower_columns(&self, columns: &[ast::ColumnDef]) -> Result<Vec<CreateColumnInfo>> {
        columns.iter().enumerate().map(|(idx, col)| self.lower_column(idx, col)).collect()
    }

    fn lower_column(&self, idx: usize, column: &ast::ColumnDef) -> Result<CreateColumnInfo> {
        ensure!(idx < u8::MAX as usize, "too many columns (max 256)");

        Ok(CreateColumnInfo {
            name: column.name.value.as_str().into(),
            index: idx as u8,
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

    fn bind_query(&self, scope: &Scope, query: &ast::Query) -> Result<(Scope, ir::TableExpr)> {
        let ast::Query { with, body, order_by, limit, offset, fetch, locks } = query;
        not_implemented!(with.is_some());
        not_implemented!(!order_by.is_empty());
        not_implemented!(offset.is_some());
        not_implemented!(fetch.is_some());
        not_implemented!(!locks.is_empty());

        let (scope, mut table_expr) = self.bind_table_expr(scope, body)?;

        if let Some(limit) = limit {
            // LIMIT is currently not allowed to reference any columns
            let limit_expr = self.bind_expr(&Scope::default(), limit)?;
            // FIXME implement general coercion mechanism between types
            let limit = match limit_expr {
                ir::Expr::Literal(lit) => match lit {
                    ir::Literal::Null => u64::MAX,
                    ir::Literal::Bool(b) => b as u64,
                    ir::Literal::Decimal(d) => d.floor().try_into().unwrap(),
                    ir::Literal::String(_) => todo!("type error"),
                },
                ir::Expr::ColumnRef(..) => {
                    unreachable!("this would have failed binding as we gave it an empty scope")
                }
            };
            table_expr = table_expr.limit(limit);
        }

        Ok((scope, table_expr))
    }

    fn bind_table_expr(
        &self,
        scope: &Scope,
        body: &ast::SetExpr,
    ) -> Result<(Scope, ir::TableExpr)> {
        let expr = match body {
            ast::SetExpr::Select(sel) => {
                let (scope, sel) = self.bind_select(scope, sel)?;
                (scope, ir::TableExpr::Selection(sel))
            }
            ast::SetExpr::Query(_) => todo!(),
            ast::SetExpr::SetOperation { .. } => todo!(),
            ast::SetExpr::Values(values) => {
                (scope.clone(), ir::TableExpr::Values(self.bind_values(scope, values)?))
            }
            ast::SetExpr::Insert(_) => todo!(),
            ast::SetExpr::Table(_) => todo!(),
            ast::SetExpr::Update(_) => todo!(),
        };

        Ok(expr)
    }

    fn bind_select(&self, scope: &Scope, select: &ast::Select) -> Result<(Scope, ir::Selection)> {
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
        not_implemented!(*distinct);
        not_implemented!(top.is_some());
        not_implemented!(into.is_some());
        not_implemented!(!lateral_views.is_empty());
        not_implemented!(selection.is_some());
        not_implemented!(!group_by.is_empty());
        not_implemented!(!cluster_by.is_empty());
        not_implemented!(!distribute_by.is_empty());
        not_implemented!(!sort_by.is_empty());
        not_implemented!(having.is_some());
        not_implemented!(qualify.is_some());

        let (scope, source) = match &from[..] {
            [] => (scope.clone(), Box::new(ir::TableExpr::Empty)),
            [table] => self.bind_joint_tables(scope, table)?,
            _ => todo!(),
        };

        let projection = projection
            .iter()
            .map(|item| self.bind_select_item(&scope, item))
            .collect::<Result<Vec<_>, _>>()?;

        Ok((scope, ir::Selection { source, projection }))
    }

    fn bind_joint_tables(
        &self,
        scope: &Scope,
        tables: &ast::TableWithJoins,
    ) -> Result<(Scope, Box<ir::TableExpr>)> {
        not_implemented!(!tables.joins.is_empty());
        let table = &tables.relation;
        self.bind_table_factor(scope, table)
    }

    fn bind_table_factor(
        &self,
        scope: &Scope,
        table: &ast::TableFactor,
    ) -> Result<(Scope, Box<ir::TableExpr>)> {
        let (scope, table_expr) = match table {
            ast::TableFactor::Table { name, alias, args, with_hints } => {
                not_implemented!(args.is_some());
                not_implemented!(!with_hints.is_empty());
                not_implemented!(alias.is_some());

                let (scope, table_ref) = self.bind_table(scope, name)?;
                (scope, ir::TableExpr::TableRef(table_ref))
            }
            ast::TableFactor::Derived { lateral, subquery, alias } => {
                not_implemented!(*lateral);
                not_implemented!(alias.is_some());

                let (scope, subquery) = self.bind_query(scope, subquery)?;
                return Ok((scope, Box::new(subquery)));
            }
            ast::TableFactor::TableFunction { .. } => todo!(),
            ast::TableFactor::UNNEST { .. } => todo!(),
            ast::TableFactor::NestedJoin { .. } => todo!(),
            ast::TableFactor::Pivot { .. } => todo!(),
        };

        Ok((scope, Box::new(table_expr)))
    }

    fn bind_table(
        &self,
        scope: &Scope,
        table_name: &ast::ObjectName,
    ) -> Result<(Scope, ir::TableRef)> {
        let table_name = self.lower_ident(&table_name.0)?;
        let (namespace, table) = self.bind_namespaced_entity::<Table>(&table_name)?;
        let table_ref = ir::TableRef { namespace, table };

        let namespace = self.catalog.get(self.tx, table_ref.namespace)?.unwrap();
        let table = namespace.get(self.tx, table_ref.table)?.unwrap();
        let columns = table.all::<Column>(self.tx)?;
        Ok((scope.bind_table(table_name, table_ref, columns), table_ref))
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
            ast::Expr::Identifier(ident) => {
                let (col, idx) = scope
                    .lookup_column(&Ident::Unqualified { name: ident.value.clone().into() })?;
                ir::Expr::ColumnRef(col, idx)
            }
            ast::Expr::CompoundIdentifier(ident) => {
                let ident = self.lower_ident(ident)?;
                let (col, idx) = scope.lookup_column(&ident)?;
                ir::Expr::ColumnRef(col, idx)
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
            ast::Value::SingleQuotedByteStringLiteral(_) => todo!(),
            ast::Value::DoubleQuotedByteStringLiteral(_) => todo!(),
            ast::Value::RawStringLiteral(_) => todo!(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
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

impl fmt::Debug for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
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
