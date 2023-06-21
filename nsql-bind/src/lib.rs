#![deny(rust_2018_idioms)]
#![feature(anonymous_lifetime_in_impl_trait)]

mod scope;

use std::collections::HashSet;
use std::str::FromStr;

pub use anyhow::Error;
use anyhow::{anyhow, bail, ensure};
use ir::expr::EvalNotConst;
use ir::{Decimal, Path, TransactionMode};
use itertools::Itertools;
use nsql_catalog::{
    Catalog, ColumnIndex, CreateColumnInfo, CreateNamespaceInfo, Namespace, SystemEntity, Table,
    MAIN_SCHEMA,
};
use nsql_core::{LogicalType, Name, Oid};
use nsql_parse::ast::{self, HiveDistributionStyle};
use nsql_storage_engine::{StorageEngine, Transaction};

use self::scope::Scope;

pub struct Binder<'env, S> {
    catalog: Catalog<'env, S>,
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

impl<'env, S: StorageEngine> Binder<'env, S> {
    pub fn new(catalog: Catalog<'env, S>) -> Self {
        Self { catalog }
    }

    pub fn requires_write_transaction(&self, storage: &S, stmt: &ast::Statement) -> Result<bool> {
        // create a temporary transaction to bind the statement to figure out what transaction mode is required to execute it
        let tmp_tx = storage.begin()?;
        let stmt = self.bind(&tmp_tx, stmt)?;
        Ok(matches!(stmt.required_transaction_mode(), TransactionMode::ReadWrite))
    }

    pub fn bind(&self, tx: &dyn Transaction<'env, S>, stmt: &ast::Statement) -> Result<ir::Stmt> {
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
                let namespace = self.bind_namespace(tx, &path)?;
                let columns = self.lower_columns(columns)?;
                if columns.iter().all(|c| !c.is_primary_key) {
                    bail!("table must have a primary key defined")
                }

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
                ir::Stmt::CreateNamespace(CreateNamespaceInfo {
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

                let mut unique_columns = HashSet::with_capacity(columns.len());
                for column in columns {
                    if !unique_columns.insert(column) {
                        bail!("duplicate column name in insert column list: `{column}`")
                    }
                }

                // We bind the columns of the table first, so that we can use them in the following projection
                let (scope, table) = self.bind_table(tx, scope, table_name, None)?;
                let (scope, mut source) = self.bind_query(tx, &scope, source)?;

                let table_columns =
                    self.catalog.get::<Table>(tx, table)?.columns(self.catalog, tx)?;
                // We model the insertion columns list as a projection over the source
                let mut projection = columns
                    .iter()
                    .map(|ident| ast::Expr::Identifier(ident.clone()))
                    .map(|expr| self.bind_expr(&scope, &expr))
                    .collect::<Result<Vec<_>>>()?;

                assert!(projection.len() <= table_columns.len());
                if !projection.is_empty() {
                    // pad any missing columns with a default value of null
                    for column in table_columns.iter().skip(projection.len()) {
                        let ty = column.logical_type().clone();
                        projection
                            .push(ir::Expr { ty, kind: ir::ExprKind::Value(ir::Value::Null) });
                    }

                    assert_eq!(projection.len(), table_columns.len());
                    source = source.project(projection);
                }

                let returning = returning
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|selection| self.bind_select_item(&scope, selection))
                            .flatten_ok()
                            .collect::<Result<_>>()
                    })
                    .transpose()?;

                ir::Stmt::Insert { table, source, returning }
            }
            ast::Statement::Query(query) => {
                let (_scope, query) = self.bind_query(tx, scope, query)?;
                ir::Stmt::Query(query)
            }
            ast::Statement::StartTransaction { modes } => {
                let mut mode = ir::TransactionMode::ReadWrite;
                match modes[..] {
                    [] => (),
                    [ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly)] => {
                        mode = ir::TransactionMode::ReadOnly
                    }
                    [ast::TransactionMode::IsolationLevel(_)] => {
                        not_implemented!("isolation level")
                    }
                    _ => not_implemented!("multiple transaction modes"),
                }
                ir::Stmt::Transaction(ir::TransactionStmtKind::Begin(mode))
            }
            ast::Statement::Rollback { chain } => {
                not_implemented!(*chain);
                ir::Stmt::Transaction(ir::TransactionStmtKind::Abort)
            }
            ast::Statement::Commit { chain } => {
                not_implemented!(*chain);
                ir::Stmt::Transaction(ir::TransactionStmtKind::Commit)
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
                            let table = self.bind_namespaced_entity::<Table>(tx, name)?;
                            Ok(ir::EntityRef::Table(table))
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
                // What does it mean to update a table with joins?
                not_implemented!(!table.joins.is_empty());
                not_implemented!(from.is_some());

                let (scope, table) = match &table.relation {
                    ast::TableFactor::Table { name, alias, args, with_hints } => {
                        not_implemented!(alias.is_some());
                        not_implemented!(args.is_some());
                        not_implemented!(!with_hints.is_empty());
                        self.bind_table(tx, scope, name, alias.as_ref())?
                    }
                    _ => not_implemented!("update with non-table relation"),
                };

                let mut source = self.build_table_scan(tx, table, None)?;
                if let Some(predicate) = selection
                    .as_ref()
                    .map(|selection| self.bind_predicate(&scope, selection))
                    .transpose()?
                {
                    source = source.filter(predicate);
                }

                let assignments = self.bind_assignments(tx, &scope, table, assignments)?;
                source = source.project(assignments);

                let returning = returning
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|selection| self.bind_select_item(&scope, selection))
                            .flatten_ok()
                            .collect::<Result<_>>()
                    })
                    .transpose()?;

                ir::Stmt::Update { table, source, returning }
            }
            ast::Statement::Explain { describe_alias: _, analyze, verbose, statement, format } => {
                not_implemented!(*analyze);
                not_implemented!(format.is_some());
                // FIXME use a session variable to decide how to print the plan rather than verbosity
                let kind = match verbose {
                    false => ir::ExplainMode::Physical,
                    true => ir::ExplainMode::Pipeline,
                };
                ir::Stmt::Explain(kind, Box::new(self.bind(tx, statement)?))
            }
            ast::Statement::SetVariable { local: _, hivevar: _, variable: _, value: _ } => todo!(),
            _ => unimplemented!("unimplemented statement: {:?}", stmt),
        };

        Ok(stmt)
    }

    fn build_table_scan(
        &self,
        tx: &dyn Transaction<'env, S>,
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Result<Box<ir::QueryPlan>> {
        let columns = self.catalog.get::<Table>(tx, table)?.columns(self.catalog, tx)?;
        let schema = columns.iter().map(|column| column.logical_type()).collect::<Vec<_>>();
        let projected_schema = projection
            .as_ref()
            .map(|projection| {
                projection.iter().map(|&index| schema[index.as_usize()].clone()).collect::<Vec<_>>()
            })
            .unwrap_or(schema);

        Ok(Box::new(ir::QueryPlan::TableScan { table, projection, projected_schema }))
    }

    fn bind_assignments(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        table: Oid<Table>,
        assignments: &[ast::Assignment],
    ) -> Result<Box<[ir::Expr]>> {
        let table = self.catalog.get::<Table>(tx, table)?;
        let columns = table.columns(self.catalog, tx)?;

        for assignment in assignments {
            assert!(!assignment.id.is_empty());
            if assignment.id.len() > 1 {
                not_implemented!("compound assignment")
            }

            if !columns.iter().any(|column| column.name().as_str() == assignment.id[0].value) {
                bail!(
                    "referenced update column `{}` does not exist in table `{}`",
                    assignment.id[0].value,
                    table.name(),
                )
            }
        }

        let mut projections = Vec::with_capacity(columns.len());
        for column in columns {
            let expr = if let Some(assignment) =
                assignments.iter().find(|assn| assn.id[0].value == column.name().as_str())
            {
                // we don't allow updating primary keys
                if column.is_primary_key() {
                    bail!(
                        "cannot update primary key column `{}` of table `{}`",
                        column.name(),
                        table.name(),
                    )
                }

                // if the column is being updated, we bind the expression in the assignment
                self.bind_expr(scope, &assignment.value)?
            } else {
                // otherwise, we bind the column to itself (effectively an identity projection)
                self.bind_expr(scope, &ast::Expr::Identifier(ast::Ident::new(column.name())))?
            };

            projections.push(expr);
        }

        Ok(projections.into_boxed_slice())
    }

    fn lower_columns(&self, columns: &[ast::ColumnDef]) -> Result<Vec<CreateColumnInfo>> {
        columns.iter().enumerate().map(|(idx, col)| self.lower_column(idx, col)).collect()
    }

    fn lower_column(&self, idx: usize, column: &ast::ColumnDef) -> Result<CreateColumnInfo> {
        not_implemented!(column.collation.is_some());
        ensure!(idx < u8::MAX as usize, "too many columns (max 256)");

        let mut is_primary_key = false;
        for option in &column.options {
            match &option.option {
                ast::ColumnOption::Unique { is_primary } if *is_primary => is_primary_key = true,
                _ => not_implemented!("column option"),
            }
        }

        Ok(CreateColumnInfo {
            name: self.lower_name(&column.name),
            index: idx as u8,
            ty: self.lower_ty(&column.data_type)?,
            is_primary_key,
        })
    }

    fn lower_ty(&self, ty: &ast::DataType) -> Result<LogicalType> {
        match ty {
            ast::DataType::Int(width) if width.is_none() => Ok(LogicalType::Int),
            ast::DataType::Boolean => Ok(LogicalType::Bool),
            ty => bail!("unhandled type: {:?}", ty),
        }
    }

    fn bind_namespace(&self, tx: &dyn Transaction<'env, S>, path: &Path) -> Result<Oid<Namespace>> {
        match path {
            Path::Qualified { prefix, .. } => match prefix.as_ref() {
                Path::Qualified { .. } => not_implemented!("qualified schemas"),
                Path::Unqualified(name) => {
                    let ns = self
                        .catalog
                        .namespaces(tx)?
                        .find(self.catalog, tx, None, name.as_str())?
                        .ok_or_else(|| unbound!(Namespace, path))?;
                    Ok(ns.key())
                }
            },
            Path::Unqualified(name) => self.bind_namespace(
                tx,
                &Path::Qualified {
                    prefix: Box::new(Path::Unqualified(MAIN_SCHEMA.into())),
                    name: name.clone(),
                },
            ),
        }
    }

    fn bind_namespaced_entity<T: SystemEntity<Parent = Namespace>>(
        &self,
        tx: &dyn Transaction<'env, S>,
        path: &Path,
    ) -> Result<T::Key> {
        match path {
            Path::Unqualified(name) => self.bind_namespaced_entity::<T>(
                tx,
                &Path::Qualified {
                    prefix: Box::new(Path::Unqualified(MAIN_SCHEMA.into())),
                    name: name.clone(),
                },
            ),
            Path::Qualified { prefix, name } => match prefix.as_ref() {
                Path::Qualified { .. } => not_implemented!("qualified schemas"),
                Path::Unqualified(schema) => {
                    let namespace = self
                        .catalog
                        .namespaces(tx)?
                        .find(self.catalog, tx, None, schema.as_str())?
                        .ok_or_else(|| unbound!(Namespace, path))?;

                    let entity = self
                        .catalog
                        .system_table::<T>(tx)?
                        .find(self.catalog, tx, Some(namespace.key()), name.as_str())?
                        .ok_or_else(|| unbound!(T, path))?;

                    Ok(entity.key())
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

    fn bind_query(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        query: &ast::Query,
    ) -> Result<(Scope<S>, Box<ir::QueryPlan>)> {
        let ast::Query { with, body, order_by, limit, offset, fetch, locks } = query;
        not_implemented!(with.is_some());
        not_implemented!(offset.is_some());
        not_implemented!(fetch.is_some());
        not_implemented!(!locks.is_empty());

        let (scope, mut table_expr) = self.bind_table_expr(tx, scope, body)?;

        if !order_by.is_empty() {
            let order_by = order_by
                .iter()
                .map(|item| {
                    let ast::OrderByExpr { expr, asc, nulls_first } = &item;
                    let expr = self.bind_expr(&scope, expr)?;
                    Ok(ir::OrderExpr {
                        expr,
                        asc: asc.unwrap_or(true),
                        nulls_first: nulls_first.unwrap_or(true),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            table_expr = table_expr.order_by(order_by);
        }

        if let Some(limit) = limit {
            // LIMIT is currently not allowed to reference any columns
            let limit_expr = self.bind_expr(&Scope::default(), limit)?;
            let limit = limit_expr
                .const_eval()
                .map_err(|EvalNotConst| anyhow!("LIMIT expression must be constant"))?
                .cast()?
                // if the limit is `NULL` we treat is as unlimited (i.e. `u64::MAX`)
                .unwrap_or(u64::MAX);
            table_expr = table_expr.limit(limit);
        }

        Ok((scope, table_expr))
    }

    fn bind_table_expr(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        body: &ast::SetExpr,
    ) -> Result<(Scope<S>, Box<ir::QueryPlan>)> {
        let (scope, expr) = match body {
            ast::SetExpr::Select(sel) => self.bind_select(tx, scope, sel)?,
            ast::SetExpr::Query(_) => todo!(),
            ast::SetExpr::SetOperation { .. } => todo!(),
            ast::SetExpr::Values(values) => {
                let (scope, values) = self.bind_values(scope, values)?;
                (scope, Box::new(ir::QueryPlan::values(values)))
            }
            ast::SetExpr::Insert(_) => todo!(),
            ast::SetExpr::Table(_) => todo!(),
            ast::SetExpr::Update(_) => todo!(),
        };

        Ok((scope, expr))
    }

    fn bind_select(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        select: &ast::Select,
    ) -> Result<(Scope<S>, Box<ir::QueryPlan>)> {
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
            named_window,
        } = select;
        not_implemented!(!named_window.is_empty());
        not_implemented!(distinct.is_some());
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
            [table] => self.bind_joint_tables(tx, scope, table)?,
            _ => todo!(),
        };

        if let Some(predicate) = selection {
            let predicate = self.bind_predicate(&scope, predicate)?;
            source = source.filter(predicate);
        }

        let projection = projection
            .iter()
            .map(|item| self.bind_select_item(&scope, item))
            .flatten_ok()
            .collect::<Result<Box<_>>>()?;

        Ok((scope, source.project(projection)))
    }

    fn bind_joint_tables(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        tables: &ast::TableWithJoins,
    ) -> Result<(Scope<S>, Box<ir::QueryPlan>)> {
        not_implemented!(!tables.joins.is_empty());
        let table = &tables.relation;
        self.bind_table_factor(tx, scope, table)
    }

    fn bind_table_factor(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        table: &ast::TableFactor,
    ) -> Result<(Scope<S>, Box<ir::QueryPlan>)> {
        let (scope, table_expr) = match table {
            ast::TableFactor::Table { name, alias, args, with_hints } => {
                not_implemented!(args.is_some());
                not_implemented!(!with_hints.is_empty());

                let (scope, table) = self.bind_table(tx, scope, name, alias.as_ref())?;
                (scope, self.build_table_scan(tx, table, None)?)
            }
            ast::TableFactor::Derived { lateral, subquery, alias } => {
                not_implemented!(*lateral);

                let (mut scope, subquery) = self.bind_query(tx, scope, subquery)?;
                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, subquery)
            }
            ast::TableFactor::TableFunction { .. } => todo!(),
            ast::TableFactor::UNNEST { alias, array_expr, with_offset, with_offset_alias } => {
                not_implemented!(*with_offset);
                not_implemented!(with_offset_alias.is_some());

                let expr = self.bind_expr(scope, array_expr)?;
                ensure!(
                    matches!(expr.ty, LogicalType::Array(_)),
                    "UNNEST expression must be an array"
                );

                let mut scope = scope.bind_unnest(&expr);

                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, Box::new(ir::QueryPlan::unnest(expr)))
            }
            ast::TableFactor::NestedJoin { .. } => todo!(),
            ast::TableFactor::Pivot { .. } => todo!(),
        };

        Ok((scope, table_expr))
    }

    fn bind_table(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope<S>,
        table_name: &ast::ObjectName,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Scope<S>, Oid<Table>)> {
        let alias = alias.map(|alias| self.lower_table_alias(alias));
        let table_name = self.lower_path(&table_name.0)?;
        let table = self.bind_namespaced_entity::<Table>(tx, &table_name)?;

        Ok((scope.bind_table(self, tx, table_name, table, alias.as_ref())?, table))
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

    fn bind_select_item(&self, scope: &Scope<S>, item: &ast::SelectItem) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_expr(scope, expr)?,
            ast::SelectItem::ExprWithAlias { expr: _, alias: _ } => todo!(),
            ast::SelectItem::QualifiedWildcard(_, _) => not_implemented!("qualified wildcard"),
            ast::SelectItem::Wildcard(ast::WildcardAdditionalOptions {
                opt_exclude,
                opt_except,
                opt_rename,
                opt_replace,
            }) => {
                not_implemented!(opt_except.is_some());
                not_implemented!(opt_rename.is_some());
                not_implemented!(opt_replace.is_some());

                let excludes = opt_exclude.as_ref().map_or(&[][..], |exclude| match exclude {
                    ast::ExcludeSelectItem::Single(name) => std::slice::from_ref(name),
                    ast::ExcludeSelectItem::Multiple(names) => &names[..],
                });

                let excludes = excludes
                    .iter()
                    .map(|ident| self.bind_ident(scope, ident).map(|(_ty, index)| index))
                    .collect::<Result<Vec<_>, _>>()?;

                let exprs = scope.column_refs(&excludes).collect::<Vec<_>>();
                if exprs.is_empty() {
                    bail!("selection list is empty after excluding columns")
                }
                return Ok(exprs);
            }
        };

        Ok(vec![expr])
    }

    fn bind_values(
        &self,
        scope: &Scope<S>,
        values: &ast::Values,
    ) -> Result<(Scope<S>, ir::Values)> {
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

        Ok((scope.bind_values(&values), values))
    }

    fn bind_row(&self, scope: &Scope<S>, row: &[ast::Expr]) -> Result<Vec<ir::Expr>> {
        row.iter().map(|expr| self.bind_expr(scope, expr)).collect()
    }

    fn bind_predicate(&self, scope: &Scope<S>, predicate: &ast::Expr) -> Result<ir::Expr> {
        let predicate = self.bind_expr(scope, predicate)?;
        // We intentionally are being strict here and only allow boolean predicates rather than
        // anything that can be cast to a boolean.
        ensure!(
            matches!(predicate.ty, LogicalType::Bool | LogicalType::Null),
            "expected predicate type of WHERE to be of type bool, got type {}",
            predicate.ty
        );

        Ok(predicate)
    }

    fn bind_ident(
        &self,
        scope: &Scope<S>,
        ident: &ast::Ident,
    ) -> Result<(LogicalType, ir::TupleIndex)> {
        scope.lookup_column(&Path::Unqualified(ident.value.clone().into()))
    }

    fn bind_expr(&self, scope: &Scope<S>, expr: &ast::Expr) -> Result<ir::Expr> {
        let (ty, kind) = match expr {
            ast::Expr::Value(literal) => self.bind_value_expr(literal),
            ast::Expr::Identifier(ident) => {
                let (ty, index) = self.bind_ident(scope, ident)?;
                (ty, ir::ExprKind::ColumnRef { path: Path::unqualified(&ident.value), index })
            }
            ast::Expr::CompoundIdentifier(ident) => {
                let path = self.lower_path(ident)?;
                let (ty, index) = scope.lookup_column(&path)?;
                (ty, ir::ExprKind::ColumnRef { path, index })
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let lhs = self.bind_expr(scope, left)?;
                let rhs = self.bind_expr(scope, right)?;
                let (ty, op) = match op {
                    ast::BinaryOperator::Eq => {
                        ensure!(
                            lhs.ty == rhs.ty,
                            "cannot compare value of type {} to {}",
                            lhs.ty,
                            rhs.ty
                        );
                        (LogicalType::Bool, ir::BinOp::Eq)
                    }
                    // ast::BinaryOperator::Plus => ir::BinOp::Add,
                    // ast::BinaryOperator::Minus => ir::BinOp::Sub,
                    // ast::BinaryOperator::Multiply => ir::BinOp::Mul,
                    // ast::BinaryOperator::Divide => ir::BinOp::Div,
                    // ast::BinaryOperator::Modulo => ir::BinOp::Mod,
                    // ast::BinaryOperator::Gt => ir::BinOp::Gt,
                    // ast::BinaryOperator::Lt => ir::BinOp::Lt,
                    // ast::BinaryOperator::GtEq => ir::BinOp::Ge,
                    // ast::BinaryOperator::LtEq => ir::BinOp::Le,
                    // ast::BinaryOperator::NotEq => ir::BinOp::Ne,
                    // ast::BinaryOperator::And => ir::BinOp::And,
                    // ast::BinaryOperator::Or => ir::BinOp::Or,
                    ast::BinaryOperator::Xor
                    | ast::BinaryOperator::StringConcat
                    | ast::BinaryOperator::Spaceship
                    | ast::BinaryOperator::BitwiseOr
                    | ast::BinaryOperator::BitwiseAnd
                    | ast::BinaryOperator::BitwiseXor
                    | ast::BinaryOperator::PGBitwiseXor
                    | ast::BinaryOperator::PGBitwiseShiftLeft
                    | ast::BinaryOperator::PGBitwiseShiftRight
                    | ast::BinaryOperator::PGExp
                    | ast::BinaryOperator::PGRegexMatch
                    | ast::BinaryOperator::PGRegexIMatch
                    | ast::BinaryOperator::PGRegexNotMatch
                    | ast::BinaryOperator::PGRegexNotIMatch
                    | ast::BinaryOperator::PGCustomBinaryOperator(_) => {
                        bail!("unsupported binary operator: {:?}", op)
                    }
                    _ => bail!("unsupported binary operator: {:?}", op),
                };

                (ty, ir::ExprKind::BinOp { op, lhs: Box::new(lhs), rhs: Box::new(rhs) })
            }
            ast::Expr::Array(array) => {
                // Keep this in sync with `Value::ty`
                let ty = match &array.elem[..] {
                    // default array type to `Int` if it is empty
                    [] => LogicalType::Int,
                    // otherwise, we can get the type from the first element
                    [first, ..] => self.bind_expr(scope, first)?.ty,
                };

                let exprs = array
                    .elem
                    .iter()
                    .map(|e| {
                        let expr = self.bind_expr(scope, e)?;
                        // FIXME this isn't going to work with `NULLs` in the array
                        ensure!(
                            expr.ty == ty,
                            "cannot create array of type {} with element of type {}",
                            ty,
                            expr.ty
                        );
                        Ok(expr)
                    })
                    .collect::<Result<Box<_>, _>>()?;

                (LogicalType::Array(Box::new(ty)), ir::ExprKind::Array(exprs))
            }
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
                    return ir::Value::Int32(i);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableAlias {
    table_name: Name,
    columns: Vec<Name>,
}
