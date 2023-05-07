#![deny(rust_2018_idioms)]

mod scope;

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

pub use anyhow::Error;
use anyhow::{anyhow, bail, ensure};
use ir::Decimal;
use itertools::Itertools;
use nsql_catalog::{
    Catalog, Container, CreateColumnInfo, Entity, Namespace, NamespaceEntity, Oid, Table,
    DEFAULT_SCHEMA,
};
use nsql_core::schema::LogicalType;
use nsql_core::Name;
use nsql_parse::ast::{self, HiveDistributionStyle};
use nsql_transaction::Transaction;

use self::scope::Scope;

pub struct Binder {
    catalog: Arc<Catalog>,
    tx: Arc<Transaction>,
}

macro_rules! not_implemented {
    ($msg:literal) => {
        anyhow::bail!("not implemented: {}", $msg)
    };
    ($cond:expr) => {
        if $cond {
            anyhow::bail!("not implemented: {}", stringify!($cond))
        }
    };
}

macro_rules! unbound {
    ($ty:ty, $path: expr) => {
        anyhow::anyhow!("unbound {} `{}`", <$ty>::desc(), $path.clone())
    };
}

use unbound;

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Binder {
    pub fn new(tx: Arc<Transaction>, catalog: Arc<Catalog>) -> Self {
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

                let path = self.lower_path(&name.0)?;
                let namespace = self.bind_namespace(&path)?;
                let columns = self.lower_columns(columns)?;
                let info = ir::CreateTableInfo { name: path.name(), namespace, columns };
                ir::Stmt::CreateTable(info)
            }
            ast::Statement::CreateSchema { schema_name, if_not_exists } => {
                not_implemented!(*if_not_exists);
                let name = match schema_name {
                    ast::SchemaName::Simple(ident) => match self.lower_path(&ident.0)? {
                        Path::Qualified { .. } => {
                            todo!("what does it mean for a schema to be qualified?")
                        }
                        Path::Unqualified(name) => name,
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
                let (scope, table_ref) = self.bind_table(scope, table_name, None)?;

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
                        .flatten_ok()
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
                    .map(|name| self.lower_path(&name.0))
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
            ast::Statement::Update { table, assignments, from, selection, returning } => {
                not_implemented!(from.is_some());
                not_implemented!(returning.is_some());
                let (scope, table) = self.bind_joint_tables(scope, table)?;
                todo!()
            }
            _ => unimplemented!("unimplemented statement: {:?}", stmt),
        };

        Ok(stmt)
    }

    fn lower_columns(&self, columns: &[ast::ColumnDef]) -> Result<Vec<CreateColumnInfo>> {
        columns.iter().enumerate().map(|(idx, col)| self.lower_column(idx, col)).collect()
    }

    fn lower_column(&self, idx: usize, column: &ast::ColumnDef) -> Result<CreateColumnInfo> {
        ensure!(idx < u8::MAX as usize, "too many columns (max 256)");

        Ok(CreateColumnInfo {
            name: self.lower_name(&column.name),
            index: idx as u8,
            ty: self.lower_ty(&column.data_type)?,
        })
    }

    fn lower_ty(&self, ty: &ast::DataType) -> Result<LogicalType> {
        match ty {
            ast::DataType::Int(width) if width.is_none() => Ok(LogicalType::Int),
            ast::DataType::Boolean => Ok(LogicalType::Bool),
            ty => bail!("unhandled type: {:?}", ty),
        }
    }

    fn bind_namespace(&self, path: &Path) -> Result<Oid<Namespace>> {
        match path {
            Path::Qualified { prefix, .. } => match prefix.as_ref() {
                Path::Qualified { .. } => not_implemented!("qualified schemas"),
                Path::Unqualified(name) => self
                    .catalog
                    .find::<Namespace>(name.as_str())?
                    // .ok_or_else(|| Error::Unbound { kind: Namespace::desc(), path: path.clone() }),
                    .ok_or_else(|| unbound!(Namespace, path)),
            },
            Path::Unqualified(name) => self.bind_namespace(&Path::Qualified {
                prefix: Box::new(Path::Unqualified(DEFAULT_SCHEMA.into())),
                name: name.clone(),
            }),
        }
    }

    fn bind_namespaced_entity<T: NamespaceEntity>(
        &self,
        path: &Path,
    ) -> Result<(Oid<Namespace>, Oid<T>)> {
        match path {
            Path::Unqualified(name) => self.bind_namespaced_entity(&Path::Qualified {
                prefix: Box::new(Path::Unqualified(DEFAULT_SCHEMA.into())),
                name: name.clone(),
            }),
            Path::Qualified { prefix, name } => match prefix.as_ref() {
                Path::Qualified { .. } => not_implemented!("qualified schemas"),
                Path::Unqualified(schema) => {
                    let (schema_oid, schema) = self
                        .catalog
                        .get_by_name::<Namespace>(&self.tx, schema.as_str())?
                        .ok_or_else(|| unbound!(Namespace, path))?;

                    let entity_oid = schema.find(name)?.ok_or_else(|| unbound!(T, path))?;

                    Ok((schema_oid, entity_oid))
                }
            },
        }
    }

    fn lower_path(&self, name: &[ast::Ident]) -> Result<Path> {
        // FIXME naive impl for now
        match name {
            [] => unreachable!("empty name?"),
            [name] => Ok(Path::Unqualified(self.lower_name(name))),
            [prefix @ .., name] => Ok(Path::Qualified {
                prefix: Box::new(self.lower_path(prefix)?),
                name: self.lower_name(name),
            }),
        }
    }

    fn bind_query(&self, scope: &Scope, query: &ast::Query) -> Result<(Scope, Box<ir::QueryPlan>)> {
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
            let limit = match limit_expr.kind {
                // if the limit is `NULL` we treat is as unlimited (i.e. `u64::MAX`)
                // FIXME we could also just not apply the limit wrapper in this case
                ir::ExprKind::Value(val) => val.cast(u64::MAX)?,
                ir::ExprKind::ColumnRef(..) => {
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
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, expr) = match body {
            ast::SetExpr::Select(sel) => self.bind_select(scope, sel)?,
            ast::SetExpr::Query(_) => todo!(),
            ast::SetExpr::SetOperation { .. } => todo!(),
            ast::SetExpr::Values(values) => {
                let (scope, values) = self.bind_values(scope, values)?;
                (scope, Box::new(ir::QueryPlan::Values(values)))
            }
            ast::SetExpr::Insert(_) => todo!(),
            ast::SetExpr::Table(_) => todo!(),
            ast::SetExpr::Update(_) => todo!(),
        };

        Ok((scope, expr))
    }

    fn bind_select(
        &self,
        scope: &Scope,
        select: &ast::Select,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
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
        not_implemented!(!group_by.is_empty());
        not_implemented!(!cluster_by.is_empty());
        not_implemented!(!distribute_by.is_empty());
        not_implemented!(!sort_by.is_empty());
        not_implemented!(having.is_some());
        not_implemented!(qualify.is_some());

        let (scope, mut source) = match &from[..] {
            [] => (scope.clone(), Box::new(ir::QueryPlan::Empty)),
            [table] => self.bind_joint_tables(scope, table)?,
            _ => todo!(),
        };

        if let Some(predicate) = selection {
            let predicate = self.bind_expr(&scope, predicate)?;
            // We intentionally are being strict here and only allow boolean predicates rather than
            // anything that can be cast to a boolean.
            ensure!(
                matches!(predicate.ty, LogicalType::Bool | LogicalType::Null),
                "expected predicate type of WHERE to be of type bool, got type {}",
                predicate.ty
            );
            source = source.filter(predicate);
        }

        let projection = projection
            .iter()
            .map(|item| self.bind_select_item(&scope, item))
            .flatten_ok()
            .collect::<Result<Vec<_>, _>>()?;

        Ok((scope, source.project(projection)))
    }

    fn bind_joint_tables(
        &self,
        scope: &Scope,
        tables: &ast::TableWithJoins,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        not_implemented!(!tables.joins.is_empty());
        let table = &tables.relation;
        self.bind_table_factor(scope, table)
    }

    fn bind_table_factor(
        &self,
        scope: &Scope,
        table: &ast::TableFactor,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, table_expr) = match table {
            ast::TableFactor::Table { name, alias, args, with_hints } => {
                not_implemented!(args.is_some());
                not_implemented!(!with_hints.is_empty());

                let (scope, table_ref) = self.bind_table(scope, name, alias.as_ref())?;
                (scope, Box::new(ir::QueryPlan::TableRef(table_ref)))
            }
            ast::TableFactor::Derived { lateral, subquery, alias } => {
                not_implemented!(*lateral);

                let (mut scope, subquery) = self.bind_query(scope, subquery)?;
                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, subquery)
            }
            ast::TableFactor::TableFunction { .. } => todo!(),
            ast::TableFactor::UNNEST { .. } => todo!(),
            ast::TableFactor::NestedJoin { .. } => todo!(),
            ast::TableFactor::Pivot { .. } => todo!(),
        };

        Ok((scope, table_expr))
    }

    fn bind_table(
        &self,
        scope: &Scope,
        table_name: &ast::ObjectName,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Scope, ir::TableRef)> {
        let alias = alias.map(|alias| self.lower_table_alias(alias));
        let table_name = self.lower_path(&table_name.0)?;
        let (namespace, table) = self.bind_namespaced_entity::<Table>(&table_name)?;
        let table_ref = ir::TableRef { namespace, table };

        Ok((scope.bind_table(self, table_name, table_ref, alias.as_ref())?, table_ref))
    }

    fn lower_table_alias(&self, alias: &ast::TableAlias) -> TableAlias {
        TableAlias {
            table_name: self.lower_name(&alias.name),
            columns: alias.columns.iter().map(|col| self.lower_name(col)).collect::<Vec<_>>(),
        }
    }

    fn lower_name(&self, name: &ast::Ident) -> Name {
        name.value.as_str().into()
    }

    fn bind_select_item(&self, scope: &Scope, item: &ast::SelectItem) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_expr(scope, expr)?,
            ast::SelectItem::ExprWithAlias { expr, alias } => todo!(),
            ast::SelectItem::QualifiedWildcard(_, _) => not_implemented!("qualified wildcard"),
            ast::SelectItem::Wildcard(ast::WildcardAdditionalOptions {
                opt_exclude,
                opt_except,
                opt_rename,
                opt_replace,
            }) => {
                not_implemented!(opt_exclude.is_some());
                not_implemented!(opt_except.is_some());
                not_implemented!(opt_rename.is_some());
                not_implemented!(opt_replace.is_some());

                return Ok(scope.column_refs().collect());
            }
        };

        Ok(vec![expr])
    }

    fn bind_values(&self, scope: &Scope, values: &ast::Values) -> Result<(Scope, ir::Values)> {
        assert!(!values.rows.is_empty(), "values can't be empty");

        let expected_cols = values.rows[0].len();
        for (i, row) in values.rows.iter().enumerate() {
            if row.len() != expected_cols {
                return Err(anyhow!(
                    "expected {} columns in row {} of VALUES clause, got {}",
                    expected_cols,
                    i,
                    row.len()
                ));
            }
        }

        let values = ir::Values::new(
            values
                .rows
                .iter()
                .map(|row| self.bind_row(scope, row))
                .collect::<Result<Vec<_>, _>>()?,
        );

        Ok((scope.bind_values(&values)?, values))
    }

    fn bind_row(&self, scope: &Scope, row: &[ast::Expr]) -> Result<Vec<ir::Expr>> {
        row.iter().map(|expr| self.bind_expr(scope, expr)).collect()
    }

    fn bind_expr(&self, scope: &Scope, expr: &ast::Expr) -> Result<ir::Expr> {
        let (ty, kind) = match expr {
            ast::Expr::Value(literal) => self.bind_value_expr(literal),
            ast::Expr::Identifier(ident) => {
                let (ty, idx) =
                    scope.lookup_column(&Path::Unqualified(ident.value.clone().into()))?;
                (ty, ir::ExprKind::ColumnRef(idx))
            }
            ast::Expr::CompoundIdentifier(ident) => {
                let ident = self.lower_path(ident)?;
                let (ty, idx) = scope.lookup_column(&ident)?;
                (ty, ir::ExprKind::ColumnRef(idx))
            }
            ast::Expr::BinaryOp { left, op, right } => todo!(),
            _ => todo!("todo expr: {:?}", expr),
        };

        Ok(ir::Expr { ty, kind })
    }

    fn bind_value_expr(&self, value: &ast::Value) -> (LogicalType, ir::ExprKind) {
        let lit = self.bind_value(value);
        (lit.ty(), ir::ExprKind::Value(lit))
    }

    fn bind_value(&self, val: &ast::Value) -> ir::Value {
        match val {
            ast::Value::Number(n, b) => {
                if let Ok(i) = n.parse::<i32>() {
                    return ir::Value::Int(i);
                }

                assert!(!b, "what does this bool mean?");
                let decimal = Decimal::from_str(n)
                    .expect("this should be a parse error if the decimal is not valid");
                ir::Value::Decimal(decimal)
            }
            ast::Value::SingleQuotedString(s) => ir::Value::Text(s.into()),
            ast::Value::DollarQuotedString(_) => todo!(),
            ast::Value::EscapedStringLiteral(_) => todo!(),
            ast::Value::NationalStringLiteral(_) => todo!(),
            ast::Value::HexStringLiteral(_) => todo!(),
            ast::Value::DoubleQuotedString(_) => todo!(),
            ast::Value::Boolean(b) => ir::Value::Bool(*b),
            ast::Value::Null => ir::Value::Null,
            ast::Value::Placeholder(_) => todo!(),
            ast::Value::UnQuotedString(_) => todo!(),
            ast::Value::SingleQuotedByteStringLiteral(_) => todo!(),
            ast::Value::DoubleQuotedByteStringLiteral(_) => todo!(),
            ast::Value::RawStringLiteral(_) => todo!(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Path {
    Qualified { prefix: Box<Path>, name: Name },
    Unqualified(Name),
}

impl Path {
    pub fn qualified(prefix: Path, name: Name) -> Path {
        Path::Qualified { prefix: Box::new(prefix), name }
    }

    pub fn prefix(&self) -> Option<&Path> {
        match self {
            Path::Qualified { prefix, .. } => Some(prefix),
            Path::Unqualified { .. } => None,
        }
    }

    pub fn name(&self) -> Name {
        match self {
            Path::Qualified { name, .. } => name.as_str().into(),
            Path::Unqualified(name) => name.as_str().into(),
        }
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Path::Qualified { prefix, name: object } => write!(f, "{prefix}.{object}"),
            Path::Unqualified(name) => write!(f, "{name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableAlias {
    table_name: Name,
    columns: Vec<Name>,
}
