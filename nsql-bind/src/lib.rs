#![deny(rust_2018_idioms)]
#![feature(try_blocks)]
#![feature(if_let_guard)]

mod function;
mod scope;
mod select;

use std::collections::HashSet;
use std::str::FromStr;

pub use anyhow::Error;
use anyhow::{anyhow, bail, ensure};
use ir::expr::EvalNotConst;
use ir::{Decimal, Path, QPath, TupleIndex};
use itertools::Itertools;
use nsql_catalog::{
    Catalog, ColumnIndex, CreateColumnInfo, CreateNamespaceInfo, Function, FunctionKind, Namespace,
    Operator, OperatorKind, SystemEntity, SystemTableView, Table, MAIN_SCHEMA,
};
use nsql_core::{LogicalType, Name, Oid, Schema};
use nsql_parse::ast;
use nsql_storage_engine::{FallibleIterator, ReadonlyExecutionMode, StorageEngine, Transaction};

use self::scope::Scope;
use self::select::SelectBinder;

pub struct Binder<'env, S> {
    catalog: Catalog<'env, S>,
}

macro_rules! not_implemented {
    ($msg:literal) => {
        anyhow::bail!("not implemented: {}", $msg)
    };
}

macro_rules! not_implemented_if {
    ($cond:expr) => {
        if $cond {
            anyhow::bail!("not implemented: {}", stringify!($cond))
        }
    };
}

use not_implemented_if;

macro_rules! unbound {
    ($ty:ty, $path:expr) => {
        anyhow::anyhow!("{} `{}` not in scope", <$ty>::desc(), $path.clone())
    };
    ($ty:ty, $path:expr) => {
        anyhow::anyhow!("{} `{}` not in scope {:?}", <$ty>::desc(), $path.clone())
    };
}

use unbound;

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl<'env, S: StorageEngine> Binder<'env, S> {
    pub fn new(catalog: Catalog<'env, S>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<Box<ir::Plan>> {
        let tx = self.catalog.storage().begin()?;
        let plan = self.bind_with(&tx, stmt)?;
        Ok(plan)
    }

    pub fn bind_with(
        &self,
        tx: &dyn Transaction<'env, S>,
        stmt: &ast::Statement,
    ) -> Result<Box<ir::Plan>> {
        let scope = &Scope::default();
        let plan = match stmt {
            ast::Statement::CreateTable {
                strict,
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
                not_implemented_if!(*strict);
                not_implemented_if!(*or_replace);
                not_implemented_if!(*temporary);
                not_implemented_if!(*external);
                not_implemented_if!(global.is_some());
                not_implemented_if!(*if_not_exists);
                not_implemented_if!(!constraints.is_empty());
                not_implemented_if!(*hive_distribution != ast::HiveDistributionStyle::NONE);
                not_implemented_if!(!table_properties.is_empty());
                not_implemented_if!(!with_options.is_empty());
                not_implemented_if!(file_format.is_some());
                not_implemented_if!(location.is_some());
                not_implemented_if!(query.is_some());
                not_implemented_if!(*without_rowid);
                not_implemented_if!(like.is_some());
                not_implemented_if!(clone.is_some());
                not_implemented_if!(engine.is_some());
                not_implemented_if!(default_charset.is_some());
                not_implemented_if!(collation.is_some());
                not_implemented_if!(on_commit.is_some());
                not_implemented_if!(on_cluster.is_some());
                not_implemented_if!(*transient);
                not_implemented_if!(order_by.is_some());

                let path = self.lower_path(&name.0)?;
                let namespace = self.bind_namespace(tx, &path)?;
                let columns = self.lower_columns(columns)?;
                if columns.iter().all(|c| !c.is_primary_key) {
                    bail!("table must have a primary key defined")
                }

                let info = ir::CreateTableInfo { name: path.name(), namespace, columns };
                ir::Plan::CreateTable(info)
            }
            ast::Statement::CreateSchema { schema_name, if_not_exists } => {
                not_implemented_if!(*if_not_exists);
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
                ir::Plan::CreateNamespace(CreateNamespaceInfo {
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
                not_implemented_if!(or.is_some());
                not_implemented_if!(*overwrite);
                not_implemented_if!(partitioned.is_some());
                not_implemented_if!(!after_columns.is_empty());
                not_implemented_if!(on.is_some());

                let mut unique_columns = HashSet::with_capacity(columns.len());
                for column in columns {
                    if !unique_columns.insert(column) {
                        bail!("duplicate column name in insert column list: `{column}`")
                    }
                }

                let (_, mut source) = self.bind_query(tx, scope, source)?;

                let (scope, table) = self.bind_table(tx, scope, table_name, None)?;

                let table_columns =
                    self.catalog.get::<Table>(tx, table)?.columns(self.catalog, tx)?;

                // if there are no columns specified, then we don't apply any projection to the source
                if !columns.is_empty() {
                    // We model the insertion columns list as a projection over the source, with missing columns filled in with nulls
                    let target_column_indices = columns
                        .iter()
                        .map(|ident| ast::Expr::Identifier(ident.clone()))
                        .map(|expr| match self.bind_expr(tx, &scope, &expr)?.kind {
                            ir::ExprKind::ColumnRef(ir::ColumnRef { index, .. }) => Ok(index),
                            _ => unreachable!("expected column reference"),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let mut projection = Vec::with_capacity(table_columns.len());
                    // loop over all columns in the table and find the corresponding column in the base projection,
                    // if one does not exist then we fill it in with a null
                    let source_schema = source.schema();
                    for column in table_columns.iter() {
                        if let Some(expr) = target_column_indices.iter().enumerate().find_map(
                            |(i, column_index)| {
                                (column_index.as_usize() == column.index().as_usize()).then_some(
                                    ir::Expr {
                                        ty: source_schema[i].clone(),
                                        kind: ir::ExprKind::ColumnRef(ir::ColumnRef {
                                            qpath: QPath::new(
                                                table.to_string().as_ref(),
                                                column.name(),
                                            ),
                                            index: TupleIndex::new(i),
                                        }),
                                    },
                                )
                            },
                        ) {
                            projection.push(expr);
                        } else {
                            let ty = column.logical_type().clone();
                            projection.push(ir::Expr {
                                ty,
                                kind: ir::ExprKind::Literal(ir::Value::Null),
                            });
                        }
                    }

                    assert_eq!(projection.len(), table_columns.len());
                    source = source.project(projection);
                }

                let source_schema = source.schema();
                ensure!(
                    table_columns.len() == source_schema.len(),
                    "table `{}` has {} columns but {} columns were supplied",
                    table_name,
                    table_columns.len(),
                    source_schema.len()
                );

                for (column, ty) in table_columns.iter().zip(source_schema) {
                    ensure!(
                        ty.is_subtype_of(&column.logical_type()),
                        "cannot insert value of type `{ty}` into column `{}` of type `{}`",
                        column.name(),
                        column.logical_type()
                    )
                }

                let returning = returning
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|selection| self.bind_select_item(tx, &scope, selection))
                            .flatten_ok()
                            .collect::<Result<Box<_>>>()
                    })
                    .transpose()?;

                let schema = match &returning {
                    Some(exprs) => exprs.iter().map(|e| e.ty.clone()).collect::<Schema>(),
                    None => Schema::empty(),
                };

                ir::Plan::query(ir::QueryPlan::Insert { table, source, returning, schema })
            }
            ast::Statement::Query(query) => {
                let (_scope, query) = self.bind_query(tx, scope, query)?;
                ir::Plan::Query(query)
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
                ir::Plan::Transaction(ir::TransactionStmt::Begin(mode))
            }
            ast::Statement::Rollback { chain } => {
                not_implemented_if!(*chain);
                ir::Plan::Transaction(ir::TransactionStmt::Abort)
            }
            ast::Statement::Commit { chain } => {
                not_implemented_if!(*chain);
                ir::Plan::Transaction(ir::TransactionStmt::Commit)
            }
            ast::Statement::ShowTables { extended, full, db_name, filter } => {
                not_implemented_if!(*extended);
                not_implemented_if!(*full);
                not_implemented_if!(db_name.is_some());
                not_implemented_if!(filter.is_some());
                ir::Plan::Show(ir::ObjectType::Table)
            }
            ast::Statement::Drop { object_type, if_exists, names, cascade, restrict, purge } => {
                not_implemented_if!(*if_exists);
                not_implemented_if!(*cascade);
                not_implemented_if!(*restrict);
                not_implemented_if!(*purge);

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

                ir::Plan::Drop(refs)
            }
            ast::Statement::Update { table, assignments, from, selection, returning } => {
                // What does it mean to update a table with joins?
                not_implemented_if!(!table.joins.is_empty());
                not_implemented_if!(from.is_some());

                let (scope, table) = match &table.relation {
                    ast::TableFactor::Table { name, alias, args, with_hints } => {
                        not_implemented_if!(alias.is_some());
                        not_implemented_if!(args.is_some());
                        not_implemented_if!(!with_hints.is_empty());
                        self.bind_table(tx, scope, name, alias.as_ref())?
                    }
                    _ => not_implemented!("update with non-table relation"),
                };

                let mut source = self.build_table_scan(tx, table, None)?;
                if let Some(predicate) = selection
                    .as_ref()
                    .map(|selection| self.bind_predicate(tx, &scope, selection))
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
                            .map(|selection| self.bind_select_item(tx, &scope, selection))
                            .flatten_ok()
                            .collect::<Result<Box<_>>>()
                    })
                    .transpose()?;

                let schema = returning
                    .as_ref()
                    .map(|exprs| exprs.iter().map(|e| e.ty.clone()).collect())
                    .unwrap_or_else(Schema::empty);

                ir::Plan::query(ir::QueryPlan::Update { table, source, returning, schema })
            }
            ast::Statement::Explain { describe_alias: _, analyze, verbose, statement, format } => {
                not_implemented_if!(*analyze);
                not_implemented_if!(format.is_some());
                not_implemented_if!(*verbose);
                ir::Plan::Explain(self.bind_with(tx, statement)?)
            }
            ast::Statement::SetVariable { local, hivevar, variable, value } => {
                not_implemented_if!(*hivevar);
                not_implemented_if!(variable.0.len() > 1);
                not_implemented_if!(value.len() > 1);

                let name = variable.0[0].value.as_str().into();
                let expr = self.bind_expr(tx, scope, &value[0])?;
                let value = match expr.const_eval() {
                    Ok(value) => value,
                    Err(EvalNotConst) => bail!("variable value must be a constant"),
                };

                ir::Plan::SetVariable {
                    name,
                    value,
                    scope: local
                        .then_some(ir::VariableScope::Local)
                        .unwrap_or(ir::VariableScope::Global),
                }
            }
            _ => unimplemented!("unimplemented statement: {:?}", stmt),
        };

        Ok(Box::new(plan))
    }

    fn build_table_scan(
        &self,
        tx: &dyn Transaction<'env, S>,
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Result<Box<ir::QueryPlan>> {
        let columns = self.catalog.get::<Table>(tx, table)?.columns(self.catalog, tx)?;
        let schema = columns.iter().map(|column| column.logical_type()).collect::<Schema>();
        let projected_schema = projection
            .as_ref()
            .map(|projection| {
                projection.iter().map(|&index| schema[index.as_usize()].clone()).collect()
            })
            .unwrap_or(schema);

        Ok(Box::new(ir::QueryPlan::TableScan { table, projection, projected_schema }))
    }

    fn bind_assignments(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
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
                self.bind_expr(tx, scope, &assignment.value)?
            } else {
                // otherwise, we bind the column to itself (effectively an identity projection)
                self.bind_expr(tx, scope, &ast::Expr::Identifier(ast::Ident::new(column.name())))?
            };

            projections.push(expr);
        }

        Ok(projections.into_boxed_slice())
    }

    fn lower_columns(&self, columns: &[ast::ColumnDef]) -> Result<Vec<CreateColumnInfo>> {
        columns.iter().enumerate().map(|(idx, col)| self.lower_column(idx, col)).collect()
    }

    fn lower_column(&self, idx: usize, column: &ast::ColumnDef) -> Result<CreateColumnInfo> {
        not_implemented_if!(column.collation.is_some());
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
            ast::DataType::Int(width) | ast::DataType::Integer(width) if width.is_none() => {
                Ok(LogicalType::Int64)
            }
            ast::DataType::Dec(info)
            | ast::DataType::Decimal(info)
            | ast::DataType::BigDecimal(info) => match info {
                ast::ExactNumberInfo::None => Ok(LogicalType::Decimal),
                ast::ExactNumberInfo::Precision(_)
                | ast::ExactNumberInfo::PrecisionAndScale(_, _) => {
                    not_implemented!("decimal precision and scale")
                }
            },
            ast::DataType::Text => Ok(LogicalType::Text),
            ast::DataType::Bytea => Ok(LogicalType::Bytea),
            ast::DataType::Boolean => Ok(LogicalType::Bool),
            ast::DataType::Float(precision) => match precision {
                Some(_) => not_implemented!("float type precision"),
                None => Ok(LogicalType::Float64),
            },
            _ => bail!("type {ty} not implemented"),
        }
    }

    fn bind_namespace(&self, tx: &dyn Transaction<'env, S>, path: &Path) -> Result<Oid<Namespace>> {
        match path {
            Path::Qualified(QPath { prefix, .. }) => match prefix.as_ref() {
                Path::Qualified(QPath { .. }) => not_implemented!("qualified schemas"),
                Path::Unqualified(name) => {
                    let ns = self
                        .catalog
                        .namespaces(tx)?
                        .find(self.catalog, tx, None, name)?
                        .ok_or_else(|| unbound!(Namespace, path))?;
                    Ok(ns.key())
                }
            },
            Path::Unqualified(name) => self.bind_namespace(
                tx,
                &Path::qualified(Path::Unqualified(MAIN_SCHEMA.into()), name.clone()),
            ),
        }
    }

    fn get_namespaced_entity_view<'txn, T: SystemEntity<Parent = Namespace>>(
        &self,
        tx: &'txn dyn Transaction<'env, S>,
        path: &Path,
    ) -> Result<(SystemTableView<'env, 'txn, S, ReadonlyExecutionMode, T>, Namespace, Name)> {
        match path {
            Path::Unqualified(name) => self.get_namespaced_entity_view::<T>(
                tx,
                &Path::qualified(Path::Unqualified(MAIN_SCHEMA.into()), name.clone()),
            ),
            Path::Qualified(QPath { prefix, name }) => match prefix.as_ref() {
                Path::Qualified(QPath { .. }) => not_implemented!("qualified schemas"),
                Path::Unqualified(schema) => {
                    let namespace = self
                        .catalog
                        .namespaces(tx)?
                        .find(self.catalog, tx, None, schema)?
                        .ok_or_else(|| unbound!(Namespace, path))?;

                    Ok((self.catalog.system_table::<T>(tx)?, namespace, name.clone()))
                }
            },
        }
    }

    /// bind an entity that lives within a namespace and whose name uniquely identifies it within the namespace
    fn bind_namespaced_entity<T: SystemEntity<Parent = Namespace, SearchKey = Name>>(
        &self,
        tx: &dyn Transaction<'env, S>,
        path: &Path,
    ) -> Result<T::Key> {
        let (table, namespace, name) = self.get_namespaced_entity_view::<T>(tx, path)?;
        let entity = table
            .find(self.catalog, tx, Some(namespace.key()), &name)?
            .ok_or_else(|| unbound!(T, path))?;

        Ok(entity.key())
    }

    fn lower_path(&self, name: &[ast::Ident]) -> Result<Path> {
        // FIXME naive impl for now
        match name {
            [] => unreachable!("empty name?"),
            [name] => Ok(Path::Unqualified(self.lower_name(name))),
            [prefix @ .., name] => {
                Ok(Path::qualified(self.lower_path(prefix)?, self.lower_name(name)))
            }
        }
    }

    fn bind_subquery(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        subquery: &ast::Query,
    ) -> Result<Box<ir::QueryPlan>> {
        let (_scope, plan) = self.bind_query(tx, scope, subquery)?;
        Ok(plan)
    }

    fn bind_query(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        query: &ast::Query,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let ast::Query { with, body, order_by, limit, offset, fetch, locks } = query;
        not_implemented_if!(with.is_some());
        not_implemented_if!(offset.is_some());
        not_implemented_if!(fetch.is_some());
        not_implemented_if!(!locks.is_empty());

        let (scope, mut table_expr) = self.bind_table_expr(tx, scope, body, order_by)?;

        if let Some(limit) = limit {
            // LIMIT is currently not allowed to reference any columns
            let limit_expr = self.bind_expr(tx, &Scope::default(), limit)?;
            if let Some(limit) = limit_expr
                .const_eval()
                .map_err(|EvalNotConst| anyhow!("LIMIT expression must be constant"))?
                .cast::<Option<u64>>()?
            {
                table_expr = table_expr.limit(limit);
            };
        }

        Ok((scope, table_expr))
    }

    fn bind_table_expr(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        body: &ast::SetExpr,
        order_by: &[ast::OrderByExpr],
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, expr) = match body {
            ast::SetExpr::Select(sel) => self.bind_select(tx, scope, sel, order_by)?,
            // FIXME we have to pass down order by into `bind_select` otherwise the projection
            // occurs before the order by which doesn't make sense (the order by won't have access to the required scope)
            // However, it also doesn't make sense to pass in the order_by for the following cases :/
            ast::SetExpr::Values(values) => {
                not_implemented_if!(!order_by.is_empty());
                let (scope, values) = self.bind_values(tx, scope, values)?;
                (scope, ir::QueryPlan::values(values))
            }
            ast::SetExpr::Query(query) => self.bind_query(tx, scope, query)?,
            ast::SetExpr::SetOperation { op, set_quantifier, left, right } => {
                not_implemented_if!(!order_by.is_empty());
                let (lhs_scope, lhs) = self.bind_table_expr(tx, scope, left, &[])?;
                let (_rhs_scope, rhs) = self.bind_table_expr(tx, scope, right, &[])?;
                let schema = if lhs.schema().is_subschema_of(rhs.schema()) {
                    rhs.schema().clone()
                } else if rhs.schema().is_subschema_of(lhs.schema()) {
                    lhs.schema().clone()
                } else {
                    bail!("schemas of set operation operands are not compatible")
                };

                let plan = match op {
                    ast::SetOperator::Union => lhs.union(schema, rhs),
                    // the others can be implemented as semi and anti joins
                    ast::SetOperator::Except => not_implemented!("except"),
                    ast::SetOperator::Intersect => not_implemented!("intersect"),
                };

                match set_quantifier {
                    ast::SetQuantifier::All => {}
                    ast::SetQuantifier::Distinct | ast::SetQuantifier::None => {
                        // push a distinct operation on top of the set operation
                        not_implemented!("distinct")
                    }
                    ast::SetQuantifier::ByName => not_implemented!("by name"),
                    ast::SetQuantifier::AllByName => not_implemented!("all by name"),
                }

                // we just pick the left scope arbitrarily
                (lhs_scope, plan)
            }
            ast::SetExpr::Insert(_) | ast::SetExpr::Table(_) | ast::SetExpr::Update(_) => {
                not_implemented_if!(!order_by.is_empty());
                todo!()
            }
        };

        Ok((scope, expr))
    }

    fn bind_select(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        select: &ast::Select,
        order_by: &[ast::OrderByExpr],
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
            named_window,
        } = select;
        not_implemented_if!(!named_window.is_empty());
        not_implemented_if!(distinct.is_some());
        not_implemented_if!(top.is_some());
        not_implemented_if!(into.is_some());
        not_implemented_if!(!lateral_views.is_empty());
        not_implemented_if!(!cluster_by.is_empty());
        not_implemented_if!(!distribute_by.is_empty());
        not_implemented_if!(!sort_by.is_empty());
        not_implemented_if!(qualify.is_some());

        let (scope, mut source) = match &from[..] {
            [] => (scope.clone(), Box::new(ir::QueryPlan::DummyScan)),
            [table] => self.bind_joint_tables(tx, scope, table)?,
            _ => todo!(),
        };

        if let Some(predicate) = selection {
            let predicate = self.bind_predicate(tx, &scope, predicate)?;
            source = source.filter(predicate);
        }

        let group_by = group_by
            .iter()
            .map(|expr| self.bind_expr(tx, &scope, expr))
            .collect::<Result<Box<_>>>()?;

        SelectBinder::new(self, group_by).bind(
            tx,
            &scope,
            source,
            projection,
            order_by,
            having.as_ref(),
        )
    }

    fn bind_joint_tables(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        tables: &ast::TableWithJoins,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (mut scope, mut plan) = self.bind_table_factor(tx, scope, &tables.relation)?;

        for join in &tables.joins {
            (scope, plan) = self.bind_join(tx, &scope, plan, join)?;
        }

        Ok((scope, plan))
    }

    fn bind_join(
        &self,
        tx: &dyn Transaction<'env, S>,
        lhs_scope: &Scope,
        lhs: Box<ir::QueryPlan>,
        join: &ast::Join,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (rhs_scope, rhs) = self.bind_table_factor(tx, &Scope::default(), &join.relation)?;

        let scope = lhs_scope.join(&rhs_scope);
        let kind = match join.join_operator {
            ast::JoinOperator::Inner(_) | ast::JoinOperator::CrossJoin => ir::JoinKind::Inner,
            ast::JoinOperator::LeftOuter(_) => ir::JoinKind::Left,
            ast::JoinOperator::RightOuter(_) => ir::JoinKind::Right,
            ast::JoinOperator::FullOuter(_) => ir::JoinKind::Full,
            _ => not_implemented!("{kind}"),
        };

        let plan = match &join.join_operator {
            ast::JoinOperator::CrossJoin | ast::JoinOperator::Inner(ast::JoinConstraint::None) => {
                lhs.cross_join(rhs)
            }
            ast::JoinOperator::Inner(constraint)
            | ast::JoinOperator::LeftOuter(constraint)
            | ast::JoinOperator::RightOuter(constraint)
            | ast::JoinOperator::FullOuter(constraint) => match constraint {
                ast::JoinConstraint::On(predicate) => {
                    let predicate = self.bind_predicate(tx, &scope, predicate)?;
                    lhs.join(kind, rhs, predicate)
                }
                ast::JoinConstraint::Using(_) => not_implemented!("using join"),
                ast::JoinConstraint::Natural => not_implemented!("natural join"),
                ast::JoinConstraint::None => {
                    bail!("must provide a join constraint for non-inner joins")
                }
            },
            ast::JoinOperator::LeftSemi(_)
            | ast::JoinOperator::RightSemi(_)
            | ast::JoinOperator::LeftAnti(_)
            | ast::JoinOperator::RightAnti(_)
            | ast::JoinOperator::CrossApply
            | ast::JoinOperator::OuterApply => not_implemented!("unsupported join type"),
        };

        Ok((scope, plan))
    }

    fn bind_table_factor(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        table: &ast::TableFactor,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, table_expr) = match table {
            ast::TableFactor::Table { name, alias, args, with_hints } => {
                not_implemented_if!(args.is_some());
                not_implemented_if!(!with_hints.is_empty());

                let (scope, table) = self.bind_table(tx, scope, name, alias.as_ref())?;
                (scope, self.build_table_scan(tx, table, None)?)
            }
            ast::TableFactor::Derived { lateral, subquery, alias } => {
                not_implemented_if!(*lateral);

                // subqueries get a fresh empty scope to prevent correlated subqueries for now
                let (mut scope, subquery) = self.bind_query(tx, &Scope::default(), subquery)?;
                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, subquery)
            }
            ast::TableFactor::TableFunction { .. } => todo!(),
            ast::TableFactor::UNNEST { alias, array_exprs, with_offset, with_offset_alias } => {
                not_implemented_if!(*with_offset);
                not_implemented_if!(with_offset_alias.is_some());
                not_implemented_if!(array_exprs.len() != 1);

                let expr = self.bind_expr(tx, scope, &array_exprs[0])?;

                let mut scope = scope.bind_unnest(&expr)?;

                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, ir::QueryPlan::unnest(expr))
            }
            ast::TableFactor::NestedJoin { .. } => todo!(),
            ast::TableFactor::Pivot { .. } => todo!(),
        };

        Ok((scope, table_expr))
    }

    fn bind_table(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        table_name: &ast::ObjectName,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Scope, Oid<Table>)> {
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

    fn bind_select_item(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        item: &ast::SelectItem,
    ) -> Result<Vec<ir::Expr>> {
        let expr = match item {
            ast::SelectItem::UnnamedExpr(expr) => self.bind_expr(tx, scope, expr)?,
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.bind_expr(tx, scope, expr)?.alias(&alias.value)
            }
            ast::SelectItem::QualifiedWildcard(_, _) => not_implemented!("qualified wildcard"),
            ast::SelectItem::Wildcard(ast::WildcardAdditionalOptions {
                opt_exclude,
                opt_except,
                opt_rename,
                opt_replace,
            }) => {
                not_implemented_if!(opt_except.is_some());
                not_implemented_if!(opt_rename.is_some());
                not_implemented_if!(opt_replace.is_some());

                let excludes = opt_exclude.as_ref().map_or(&[][..], |exclude| match exclude {
                    ast::ExcludeSelectItem::Single(name) => std::slice::from_ref(name),
                    ast::ExcludeSelectItem::Multiple(names) => &names[..],
                });

                let excludes = excludes
                    .iter()
                    .map(|ident| self.bind_ident(scope, ident).map(|(_qpath, _ty, index)| index))
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
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        values: &ast::Values,
    ) -> Result<(Scope, ir::Values)> {
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
                .map(|row| self.bind_row(tx, scope, row))
                .collect::<Result<Box<_>, _>>()?,
        );

        Ok((scope.bind_values(&values), values))
    }

    fn bind_row(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        row: &[ast::Expr],
    ) -> Result<Box<[ir::Expr]>> {
        row.iter().map(|expr| self.bind_expr(tx, scope, expr)).collect()
    }

    fn bind_predicate(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        predicate: &ast::Expr,
    ) -> Result<ir::Expr> {
        let predicate = self.bind_expr(tx, scope, predicate)?;
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
        scope: &Scope,
        ident: &ast::Ident,
    ) -> Result<(QPath, LogicalType, ir::TupleIndex)> {
        scope.lookup_column(&Path::Unqualified(ident.value.clone().into()))
    }

    fn bind_function(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        f: &ast::Function,
    ) -> Result<(ir::MonoFunction, Box<[ir::Expr]>)> {
        let ast::Function { name, args, over, distinct, special, order_by } = f;
        not_implemented_if!(over.is_some());
        not_implemented_if!(*distinct);
        not_implemented_if!(*special);
        not_implemented_if!(!order_by.is_empty());

        let args = args
            .iter()
            // filter out any wildcard parameters, we just treat it like it's not there for now
            // i.e. `COUNT(*)` desugars to just `COUNT()`
            .filter(|arg| !matches!(arg, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)))
            .map(|arg| {
                Ok(match arg {
                    ast::FunctionArg::Named { .. } => {
                        not_implemented!("named function args")
                    }
                    ast::FunctionArg::Unnamed(expr) => match expr {
                        ast::FunctionArgExpr::Expr(expr) => expr,
                        ast::FunctionArgExpr::QualifiedWildcard(_) => {
                            not_implemented!("qualified wildcard arg")
                        }
                        ast::FunctionArgExpr::Wildcard => unreachable!("filtered out above"),
                    },
                })
            })
            .collect::<Result<Vec<&ast::Expr>, _>>()?;

        let args =
            args.iter().map(|arg| self.bind_expr(tx, scope, arg)).collect::<Result<Box<_>, _>>()?;
        let arg_types = args.iter().map(|arg| arg.ty.clone()).collect::<Box<_>>();
        let path = self.lower_path(&name.0)?;
        let function = self.resolve_function(tx, &path, &arg_types)?;
        Ok((function, args))
    }

    fn bind_unary_operator(
        &self,
        tx: &dyn Transaction<'env, S>,
        name: &'static str,
        operand: LogicalType,
    ) -> Result<ir::MonoOperator> {
        self.bind_operator(tx, name, OperatorKind::Unary, LogicalType::Null, operand)
    }

    fn bind_binary_operator(
        &self,
        tx: &dyn Transaction<'env, S>,
        name: &'static str,
        left: LogicalType,
        right: LogicalType,
    ) -> Result<ir::MonoOperator> {
        self.bind_operator(tx, name, OperatorKind::Binary, left, right)
    }

    fn bind_operator(
        &self,
        tx: &dyn Transaction<'env, S>,
        name: &'static str,
        kind: OperatorKind,
        left: LogicalType,
        right: LogicalType,
    ) -> Result<ir::MonoOperator> {
        if matches!(kind, OperatorKind::Unary) {
            assert!(matches!(left, LogicalType::Null));
        }

        // handle null type defaulting
        let (left, right) = match (kind, left, right) {
            // default to `int64` if arguments are null
            (_, LogicalType::Null, LogicalType::Null) => (LogicalType::Int64, LogicalType::Int64),
            (OperatorKind::Unary, LogicalType::Null, ty) => (LogicalType::Null, ty),
            (OperatorKind::Binary, LogicalType::Null, ty) => (ty.clone(), ty),
            (_, ty, LogicalType::Null) => (ty.clone(), ty),
            (_, left, right) => (left, right),
        };

        let (operators, namespace, name) = self
            .get_namespaced_entity_view::<Operator>(tx, &Path::qualified("nsql_catalog", name))?;

        let mut candidates = operators
            .scan()?
            .filter(|op| {
                Ok(op.parent_oid(self.catalog, tx)?.unwrap() == namespace.key()
                    && op.name() == name)
            })
            .peekable();

        if candidates.peek()?.is_none() {
            return Err(unbound!(Operator, name));
        }

        let operator = match kind {
            OperatorKind::Unary => {
                let args = [right];
                self.resolve_candidate_operators(tx, candidates, &args)?.ok_or_else(|| {
                    let [right] = args;
                    anyhow!("no operator overload for `{name}{right}`")
                })?
            }
            OperatorKind::Binary => {
                let args = [left, right];
                self.resolve_candidate_operators(tx, candidates, &args)?.ok_or_else(|| {
                    let [left, right] = args;
                    anyhow!("no operator overload for `{left} {name} {right}`")
                })?
            }
        };

        Ok(operator)
    }

    fn resolve_function(
        &self,
        tx: &dyn Transaction<'env, S>,
        path: &Path,
        args: &[LogicalType],
    ) -> Result<ir::MonoFunction> {
        let (functions, namespace, name) = self.get_namespaced_entity_view::<Function>(tx, path)?;
        let mut candidates = functions
            .scan()?
            .filter(|f| {
                Ok(f.parent_oid(self.catalog, tx)?.unwrap() == namespace.key() && f.name() == name)
            })
            .peekable();

        if candidates.peek()?.is_none() {
            return Err(unbound!(Function, path));
        }

        let function = self.resolve_candidate_functions(candidates, args)?.ok_or_else(|| {
            anyhow!(
                "no `{path}` function overload matches argument types ({})",
                args.iter().format(", ")
            )
        })?;

        Ok(function)
    }

    fn walk_expr(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        expr: &ast::Expr,
        mut f: impl FnMut(&ast::Expr) -> Result<ir::Expr>,
    ) -> Result<ir::Expr> {
        let (ty, kind) = match expr {
            ast::Expr::Value(literal) => self.bind_value_expr(literal),
            ast::Expr::Identifier(ident) => {
                let (qpath, ty, index) = self.bind_ident(scope, ident)?;
                (ty, ir::ExprKind::ColumnRef(ir::ColumnRef { qpath, index }))
            }
            ast::Expr::CompoundIdentifier(ident) => {
                let path = self.lower_path(ident)?;
                let (qpath, ty, index) = scope.lookup_column(&path)?;
                (ty, ir::ExprKind::ColumnRef(ir::ColumnRef { qpath, index }))
            }
            ast::Expr::UnaryOp { op, expr } => {
                let expr = Box::new(f(expr)?);
                let op = match op {
                    ast::UnaryOperator::Plus => todo!(),
                    ast::UnaryOperator::Minus => Operator::MINUS,
                    ast::UnaryOperator::Not => Operator::NOT,
                    ast::UnaryOperator::PGBitwiseNot => todo!(),
                    ast::UnaryOperator::PGSquareRoot => todo!(),
                    ast::UnaryOperator::PGCubeRoot => todo!(),
                    ast::UnaryOperator::PGPostfixFactorial => todo!(),
                    ast::UnaryOperator::PGPrefixFactorial => todo!(),
                    ast::UnaryOperator::PGAbs => todo!(),
                };

                let operator = self.bind_unary_operator(tx, op, expr.ty())?;
                (operator.return_type(), ir::ExprKind::UnaryOperator { operator, expr })
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let lhs = Box::new(f(left)?);
                let rhs = Box::new(f(right)?);

                let op = match op {
                    ast::BinaryOperator::Eq => Operator::EQUAL,
                    ast::BinaryOperator::NotEq => Operator::NOT_EQUAL,
                    ast::BinaryOperator::Plus => Operator::PLUS,
                    ast::BinaryOperator::Minus => Operator::MINUS,
                    ast::BinaryOperator::Lt => Operator::LESS,
                    ast::BinaryOperator::LtEq => Operator::LESS_EQUAL,
                    ast::BinaryOperator::GtEq => Operator::GREATER_EQUAL,
                    ast::BinaryOperator::Gt => Operator::GREATER,
                    ast::BinaryOperator::Multiply => Operator::STAR,
                    ast::BinaryOperator::Divide => Operator::SLASH,
                    // the compiler should special case these operators and implement short circuiting
                    ast::BinaryOperator::And => Operator::AND,
                    ast::BinaryOperator::Or => Operator::OR,
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

                let operator = self.bind_binary_operator(tx, op, lhs.ty(), rhs.ty())?;
                (operator.return_type(), ir::ExprKind::BinaryOperator { operator, lhs, rhs })
            }
            ast::Expr::Cast { expr, data_type } => {
                let expr = f(expr)?;
                let target = self.lower_ty(data_type)?;
                // We implement `CAST` by hacking operator overloading.
                // We search for a cast function `(from, target) -> target` with the second parameter being a dummy argument.
                let args = [expr.ty(), target];
                let function =
                    self.resolve_function(tx, &Path::qualified(MAIN_SCHEMA, "cast"), &args)?;
                let [_, target] = args;
                (
                    function.return_type(),
                    ir::ExprKind::FunctionCall {
                        function,
                        args: [
                            expr,
                            // just pass null as the dummy argument as it is unused
                            ir::Expr { ty: target, kind: ir::ExprKind::Literal(ir::Value::Null) },
                        ]
                        .into(),
                    },
                )
            }
            ast::Expr::Array(array) => {
                let mut expected_ty = None;
                let exprs = array
                    .elem
                    .iter()
                    .map(|e| {
                        let expr = f(e)?;
                        if let Some(expected) = &expected_ty {
                            ensure!(
                                &expr.ty.is_subtype_of(expected),
                                "cannot create array of type {} with element of type {}",
                                expected,
                                expr.ty
                            );
                        } else if !expr.ty.is_null() {
                            expected_ty = Some(expr.ty.clone());
                        }

                        Ok(expr)
                    })
                    .collect::<Result<Box<_>, _>>()?;

                // default type to `Int64` if there are no non-null elements
                let ty = expected_ty.unwrap_or(LogicalType::Int64);
                (LogicalType::Array(Box::new(ty)), ir::ExprKind::Array(exprs))
            }
            ast::Expr::Function(f) => {
                let (function, args) = self.bind_function(tx, scope, f)?;
                let return_type = function.return_type();
                match function.kind() {
                    FunctionKind::Scalar => {
                        (return_type, ir::ExprKind::FunctionCall { function, args })
                    }
                    FunctionKind::Aggregate => bail!("aggregate not allowed here"),
                }
            }
            ast::Expr::Subquery(subquery) => {
                let plan = self.bind_subquery(tx, scope, subquery)?;

                let schema = plan.schema();
                ensure!(
                    schema.len() == 1,
                    "subquery expression must return exactly one column, got {}",
                    schema.len()
                );

                let ty = schema[0].clone();
                (ty, ir::ExprKind::Subquery(ir::SubqueryKind::Scalar, plan))
            }
            ast::Expr::Exists { subquery, negated } => {
                // `NOT EXISTS (subquery)` is literally `NOT (EXISTS (subquery))` so do that transformation
                if *negated {
                    return self.walk_expr(
                        tx,
                        scope,
                        &ast::Expr::UnaryOp {
                            op: ast::UnaryOperator::Not,
                            expr: Box::new(ast::Expr::Exists {
                                subquery: subquery.clone(),
                                negated: false,
                            }),
                        },
                        f,
                    );
                }

                let plan = self.bind_subquery(tx, scope, subquery)?;
                (LogicalType::Bool, ir::ExprKind::Subquery(ir::SubqueryKind::Exists, plan))
            }
            ast::Expr::Case { operand, conditions, results, else_result } => {
                assert_eq!(conditions.len(), results.len());
                assert!(!conditions.is_empty());

                let scrutinee = match operand {
                    Some(scrutinee) => f(scrutinee)?,
                    // if there is no scrutinee, we can just use a literal `true` to compare each branch against
                    None => ir::Expr {
                        ty: LogicalType::Bool,
                        kind: ir::ExprKind::Literal(ir::Value::Bool(true)),
                    },
                };

                let cases = conditions
                    .iter()
                    .zip(results)
                    .map(|(when, then)| {
                        let when = f(when)?;
                        ensure!(
                            when.ty == scrutinee.ty,
                            "case condition must match type of scrutinee, expected `{}`, got `{}`",
                            scrutinee.ty,
                            when.ty
                        );

                        let then = f(then)?;
                        Ok(ir::Case { when, then })
                    })
                    .collect::<Result<Box<_>, _>>()?;

                let else_result = else_result.as_ref().map(|r| f(r).map(Box::new)).transpose()?;

                let result_type = cases[0].then.ty.clone();
                for expr in
                    cases.iter().map(|case| &case.then).chain(else_result.iter().map(AsRef::as_ref))
                {
                    ensure!(
                        expr.ty == result_type,
                        "all case results must have the same type, expected `{}`, got `{}`",
                        result_type,
                        expr.ty
                    );
                }

                (
                    result_type,
                    ir::ExprKind::Case { scrutinee: Box::new(scrutinee), cases, else_result },
                )
            }
            _ => todo!("todo expr: {:?}", expr),
        };

        Ok(ir::Expr { ty, kind })
    }

    fn bind_expr(
        &self,
        tx: &dyn Transaction<'env, S>,
        scope: &Scope,
        expr: &ast::Expr,
    ) -> Result<ir::Expr> {
        self.walk_expr(tx, scope, expr, |expr| self.bind_expr(tx, scope, expr))
    }

    fn bind_value_expr(&self, value: &ast::Value) -> (LogicalType, ir::ExprKind) {
        let value = self.bind_value(value);
        (Self::default_logical_type_of_value(&value), ir::ExprKind::Literal(value))
    }

    // resist moving this logic out of the binder.
    // the array defaulting logic is a bit of a hack that will cause issues if used outside of here.
    // i.e. if we have an empty array and someone decides to call say `val.logical_type()` it will
    // be `int[]`, which is probably not the proper type.
    fn default_logical_type_of_value(value: &ir::Value) -> LogicalType {
        match value {
            ir::Value::Null => LogicalType::Null,
            ir::Value::Int64(_) => LogicalType::Int64,
            ir::Value::Bool(_) => LogicalType::Bool,
            ir::Value::Decimal(_) => LogicalType::Decimal,
            ir::Value::Text(_) => LogicalType::Text,
            ir::Value::Oid(_) => LogicalType::Oid,
            ir::Value::Bytea(_) => LogicalType::Bytea,
            ir::Value::Array(values) => LogicalType::array(
                values
                    .iter()
                    .find(|val| !matches!(val, ir::Value::Null))
                    .map(|val| Self::default_logical_type_of_value(val))
                    .unwrap_or(LogicalType::Int64),
            ),
            ir::Value::Type(_) => LogicalType::Type,
            ir::Value::TupleExpr(_) => LogicalType::TupleExpr,
            ir::Value::Byte(_) => LogicalType::Byte,
            ir::Value::Float64(_) => LogicalType::Float64,
        }
    }

    fn bind_value(&self, val: &ast::Value) -> ir::Value {
        match val {
            ast::Value::Number(n, b) => {
                assert!(!b, "what does this bool mean?");
                if let Ok(i) = n.parse::<i64>() {
                    return ir::Value::Int64(i);
                }

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
