#![deny(rust_2018_idioms)]
#![feature(try_blocks)]
#![feature(if_let_guard)]

mod function;
mod scope;
mod select;

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

pub use anyhow::Error;
use anyhow::{anyhow, bail, ensure};
use ir::expr::EvalNotConst;
use ir::{Decimal, Path, QPath, TupleIndex};
use itertools::Itertools;
use nsql_catalog::{
    Bow, Catalog, ColumnIdentity, ColumnIndex, Function, FunctionKind, Namespace, Operator,
    OperatorKind, SystemEntity, SystemTableView, Table, TransactionContext, MAIN_SCHEMA_PATH,
};
use nsql_core::{not_implemented, not_implemented_if, LogicalType, Name, Oid, Schema};
use nsql_parse::ast;
use nsql_storage::expr;
use nsql_storage_engine::{ExecutionMode, FallibleIterator, StorageEngine};

use self::scope::{CteKind, Scope, TableBinding};
use self::select::SelectBinder;

pub struct Binder<'a, 'env, 'txn, S, M> {
    catalog: Catalog<'env, S>,
    tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ctes: RefCell<HashMap<Name, CteMeta>>,
}

type CteMeta = (CteKind, Scope, Box<ir::QueryPlan>);

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

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    Binder<'a, 'env, 'txn, S, M>
{
    pub fn new(
        catalog: Catalog<'env, S>,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Self {
        Self { catalog, tx, ctes: Default::default() }
    }

    pub fn bind(&self, stmt: &ast::Statement) -> Result<Box<ir::Plan>> {
        let scope = &Scope::default();
        let plan = match stmt {
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

                let table = Table::NAMESPACE;
                let columns =
                    self.catalog.get::<M, Table>(self.tx, table)?.columns(self.catalog, self.tx)?;
                assert_eq!(columns.len(), 2);
                let table_oid_column = &columns[0];
                assert_eq!(table_oid_column.name(), "oid");

                let source = ir::QueryPlan::values(ir::Values::new(
                    [[
                        ir::Expr {
                            ty: LogicalType::Oid,
                            kind: ir::ExprKind::Compiled(table_oid_column.default_expr().clone()),
                        },
                        literal(name),
                    ]
                    .into()]
                    .into(),
                ));

                ir::Plan::query(ir::QueryPlan::Insert {
                    table,
                    source,
                    returning: [].into(),
                    schema: Schema::empty(),
                })
            }
            ast::Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
            } => {
                not_implemented_if!(*temporary);
                not_implemented_if!(*if_not_exists);
                not_implemented_if!(owned_by.is_some());
                not_implemented_if!(data_type.is_some());

                let mut start = 1;
                let mut step = 1;

                for sequence_option in sequence_options {
                    match sequence_option {
                        ast::SequenceOptions::IncrementBy(expr, _) => {
                            let expr = self.bind_expr(scope, expr)?;
                            step = expr
                                .const_eval()
                                .or_else(|EvalNotConst| {
                                    bail!("sequence increment value must be constant")
                                })?
                                .cast()?;
                        }
                        ast::SequenceOptions::StartWith(expr, _) => {
                            let expr = self.bind_expr(scope, expr)?;
                            start = expr
                                .const_eval()
                                .or_else(|EvalNotConst| {
                                    bail!("sequence start value must be constant")
                                })?
                                .cast()?;
                        }
                        ast::SequenceOptions::MinValue(value) => match value {
                            ast::MinMaxValue::Empty | ast::MinMaxValue::None => {}
                            ast::MinMaxValue::Some(_) => not_implemented!("sequence min value"),
                        },
                        ast::SequenceOptions::MaxValue(value) => match value {
                            ast::MinMaxValue::Empty | ast::MinMaxValue::None => {}
                            ast::MinMaxValue::Some(_) => not_implemented!("sequence max value"),
                        },
                        ast::SequenceOptions::Cache(_) => not_implemented!("sequence cache"),
                        ast::SequenceOptions::Cycle(yes) => {
                            if *yes {
                                not_implemented!("sequence cycle")
                            }
                        }
                    }
                }

                let path = self.lower_path(&name.0)?;
                let namespace = &self.bind_namespace(&path)?;
                let name = path.name();

                ir::Plan::Query(self.bind_create_sequence(&CreateSequenceInfo {
                    namespace,
                    name,
                    start,
                    step,
                })?)
            }
            ast::Statement::CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
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
                order_by,
                strict,
                comment,
                auto_increment_offset,
            } => {
                not_implemented_if!(comment.is_some());
                not_implemented_if!(auto_increment_offset.is_some());
                not_implemented_if!(*strict);
                not_implemented_if!(*or_replace);
                not_implemented_if!(*temporary);
                not_implemented_if!(*external);
                not_implemented_if!(global.is_some());
                not_implemented_if!(*if_not_exists);
                not_implemented_if!(*hive_distribution != ast::HiveDistributionStyle::NONE);
                not_implemented_if!(hive_formats.is_none());
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
                let namespace = self.bind_namespace(&path)?;
                let pk_constraints = self.lower_table_constraints(columns, constraints)?;
                let columns = self.lower_columns(&path, columns, pk_constraints)?;
                let name = path.name();

                let plan = self.bind_create_table(&namespace, name, columns)?;
                ir::Plan::Query(plan)
            }
            ast::Statement::Insert { .. } => {
                let (_scope, query) = self.bind_insert(scope, stmt)?;
                ir::Plan::Query(query)
            }
            ast::Statement::Update { .. } => {
                let (_scope, query) = self.bind_update(scope, stmt)?;
                ir::Plan::Query(query)
            }
            ast::Statement::Query(query) => {
                let (_scope, query) = self.bind_query(scope, query)?;
                ir::Plan::Query(query)
            }
            ast::Statement::StartTransaction { modes, begin: _ } => {
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
            ast::Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
            } => {
                not_implemented_if!(*if_exists);
                not_implemented_if!(*cascade);
                not_implemented_if!(*restrict);
                not_implemented_if!(*purge);
                not_implemented_if!(*temporary);

                let names = names
                    .iter()
                    .map(|name| self.lower_path(&name.0))
                    .collect::<Result<Vec<_>>>()?;

                let refs = names
                    .iter()
                    .map(|name| match object_type {
                        ast::ObjectType::Table => {
                            let table = self.bind_namespaced_entity::<Table>(name)?;
                            Ok(ir::EntityRef::Table(table.key()))
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
            ast::Statement::Explain {
                describe_alias: _,
                analyze,
                verbose,
                statement,
                format,
                timing,
            } => {
                not_implemented_if!(format.is_some());

                let opts =
                    ir::ExplainOptions { analyze: *analyze, verbose: *verbose, timing: *timing };
                ir::Plan::Explain(opts, self.bind(statement)?)
            }
            ast::Statement::SetVariable { local, hivevar, variable, value } => {
                not_implemented_if!(*hivevar);
                not_implemented_if!(variable.0.len() > 1);
                not_implemented_if!(value.len() > 1);

                let name = variable.0[0].value.as_str().into();
                let expr = self.bind_expr(scope, &value[0])?;
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
            ast::Statement::Copy { source, to, target, options, legacy_options, values } => {
                not_implemented_if!(!legacy_options.is_empty());
                not_implemented_if!(!options.is_empty());
                not_implemented_if!(!values.is_empty());

                let src = match source {
                    ast::CopySource::Table { table_name, columns } => {
                        let name = table_name.clone();
                        let (scope, mut plan) = self.bind_table_factor(
                            &Scope::default(),
                            &ast::TableFactor::Table {
                                name,
                                alias: None,
                                args: None,
                                with_hints: vec![],
                                partitions: vec![],
                                version: None,
                            },
                        )?;

                        if !columns.is_empty() {
                            let projection = columns
                                .iter()
                                .map(|name| {
                                    self.bind_expr(&scope, &ast::Expr::Identifier(name.clone()))
                                })
                                .collect::<Result<Box<_>>>()?;
                            plan = plan.project(projection);
                        }

                        plan
                    }
                    ast::CopySource::Query(query) => self.bind_query(scope, query)?.1,
                };

                let dst = match target {
                    ast::CopyTarget::Stdin => not_implemented!("stdin"),
                    ast::CopyTarget::Stdout => not_implemented!("stdout"),
                    ast::CopyTarget::File { filename } => {
                        ir::CopyDestination::File(filename.into())
                    }
                    ast::CopyTarget::Program { .. } => not_implemented!("program"),
                };

                let copy = if *to {
                    ir::Copy::To(ir::CopyTo { src, dst })
                } else {
                    not_implemented!("copy from")
                };

                ir::Plan::Copy(copy)
            }
            _ => unimplemented!("unimplemented statement: {:?}", stmt),
        };

        Ok(Box::new(plan))
    }

    fn build_table_scan(
        &self,
        table: Oid<Table>,
        projection: Option<Box<[ColumnIndex]>>,
    ) -> Result<Box<ir::QueryPlan>> {
        let columns =
            self.catalog.get::<M, Table>(self.tx, table)?.columns(self.catalog, self.tx)?;
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
        scope: &Scope,
        table: Oid<Table>,
        assignments: &[ast::Assignment],
    ) -> Result<Box<[ir::Expr]>> {
        let table = self.catalog.get::<M, Table>(self.tx, table)?;
        let columns = table.columns(self.catalog, self.tx)?;

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
        for column in &columns[..] {
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

    fn lower_table_constraints(
        &self,
        column_defs: &[ast::ColumnDef],
        constraints: &[ast::TableConstraint],
    ) -> Result<Vec<PrimaryKeyConstraint>> {
        let mut primary_key_constraints = vec![];
        for constraint in constraints {
            match constraint {
                ast::TableConstraint::Unique { name: _, columns, is_primary } => {
                    if !is_primary {
                        bail!("unique constraints")
                    }

                    // FIXME short term implementation just for PKs, we need to store metadata about this in the catalog etc
                    let column_indices = columns
                        .iter()
                        .map(|col| {
                            column_defs.iter().position(|def| col == &def.name).ok_or_else(|| {
                                anyhow!("column `y` named in primary key does not exist")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    primary_key_constraints.push(PrimaryKeyConstraint { column_indices });
                }
                ast::TableConstraint::ForeignKey { .. } => {
                    not_implemented!("foreign key constraints")
                }
                ast::TableConstraint::Check { .. } => not_implemented!("check constraints"),
                ast::TableConstraint::Index { .. } => not_implemented!("index constraints"),
                ast::TableConstraint::FulltextOrSpatial { .. } => not_implemented!("constraints"),
            }
        }

        Ok(primary_key_constraints)
    }

    fn lower_columns(
        &self,
        table_path: &Path,
        columns: &[ast::ColumnDef],
        mut primary_key_constraints: Vec<PrimaryKeyConstraint>,
    ) -> Result<Vec<CreateColumnInfo>> {
        ensure!(columns.len() < u8::MAX as usize, "too many columns (max 256)");

        let mut columns = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| {
                not_implemented_if!(column.collation.is_some());

                let mut identity = ColumnIdentity::None;
                let mut default_expr = None;

                for option in &column.options {
                    match &option.option {
                        ast::ColumnOption::Unique { is_primary } if *is_primary => {
                            primary_key_constraints
                                .push(PrimaryKeyConstraint { column_indices: vec![idx] })
                        }
                        ast::ColumnOption::Generated {
                            generated_as,
                            sequence_options,
                            generation_expr,
                        } => {
                            not_implemented_if!(generation_expr.is_some());
                            match generated_as {
                                ast::GeneratedAs::Always => identity = ColumnIdentity::Always,
                                ast::GeneratedAs::ByDefault => identity = ColumnIdentity::ByDefault,
                                ast::GeneratedAs::ExpStored => {
                                    not_implemented!("generated expression stored")
                                }
                            }

                            match sequence_options {
                                Some(options) if !options.is_empty() => {
                                    not_implemented!("sequence options")
                                }
                                _ => (),
                            }
                        }
                        ast::ColumnOption::Default(expr) => {
                            default_expr = Some(self.bind_expr(&Scope::default(), expr)?)
                        }
                        _ => not_implemented!("column option"),
                    }
                }

                Ok(CreateColumnInfo {
                    name: self.lower_name(&column.name),
                    ty: self.lower_ty(&column.data_type)?,
                    identity,
                    is_primary_key: false, // this will be filled in after
                    default_expr,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        match &primary_key_constraints[..] {
            [] => bail!("table `{table_path}` must have a primary key"),
            [pk] => pk.column_indices.iter().for_each(|&idx| {
                columns[idx].is_primary_key = true;
            }),

            _ => bail!("multiple primary keys for table `{table_path}` are not allowed"),
        };

        Ok(columns)
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
            ast::DataType::Custom(name, args) if args.is_empty() && name.0.len() == 1 => {
                match name.0[0].value.as_str() {
                    "oid" => Ok(LogicalType::Oid),
                    _ => not_implemented!("custom type"),
                }
            }
            _ => bail!("type {ty:?} not implemented"),
        }
    }

    fn bind_namespace(&self, path: &Path) -> Result<Namespace> {
        match path {
            Path::Qualified(QPath { prefix, .. }) => match prefix.as_ref() {
                Path::Qualified(QPath { .. }) => not_implemented!("qualified schemas"),
                Path::Unqualified(name) => {
                    let ns = self
                        .catalog
                        .namespaces(self.tx)?
                        .as_ref()
                        .find(self.catalog, self.tx, None, name)?
                        .ok_or_else(|| unbound!(Namespace, path))?;
                    Ok(ns)
                }
            },
            Path::Unqualified(name) => self.bind_namespace(&Path::qualified(
                Path::Unqualified(MAIN_SCHEMA_PATH.into()),
                name.clone(),
            )),
        }
    }

    #[allow(clippy::type_complexity)]
    fn get_namespaced_entity_view<T: SystemEntity<Parent = Namespace>>(
        &self,
        path: &Path,
    ) -> Result<(Bow<'_, SystemTableView<'env, 'txn, S, M, T>>, Namespace, Name)> {
        match path {
            Path::Unqualified(name) => self.get_namespaced_entity_view::<T>(&Path::qualified(
                Path::Unqualified(MAIN_SCHEMA_PATH.into()),
                name.clone(),
            )),
            Path::Qualified(QPath { prefix, name }) => match prefix.as_ref() {
                Path::Qualified(QPath { .. }) => not_implemented!("qualified schemas"),
                Path::Unqualified(schema) => {
                    let namespace = self
                        .catalog
                        .namespaces(self.tx)?
                        .as_ref()
                        .find(self.catalog, self.tx, None, schema)?
                        .ok_or_else(|| unbound!(Namespace, path))?;

                    Ok((self.catalog.system_table::<M, T>(self.tx)?, namespace, name.clone()))
                }
            },
        }
    }

    /// bind an entity that lives within a namespace and whose name uniquely identifies it within the namespace
    fn bind_namespaced_entity<T: SystemEntity<Parent = Namespace, SearchKey = Name>>(
        &self,
        path: &Path,
    ) -> Result<T> {
        let (table, namespace, name) = self.get_namespaced_entity_view::<T>(path)?;
        let entity = table
            .as_ref()
            .find(self.catalog, self.tx, Some(namespace.key()), &name)?
            .ok_or_else(|| unbound!(T, path))?;

        Ok(entity)
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
        scope: &Scope,
        subquery: &ast::Query,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        self.bind_query(&scope.subscope(), subquery)
    }

    fn bind_query(&self, scope: &Scope, query: &ast::Query) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let ast::Query { with, body, order_by, limit, offset, fetch, locks, limit_by } = query;
        not_implemented_if!(offset.is_some());
        not_implemented_if!(fetch.is_some());
        not_implemented_if!(!locks.is_empty());
        not_implemented_if!(!limit_by.is_empty());

        let mut ctes = Vec::with_capacity(with.as_ref().map_or(0, |with| with.cte_tables.len()));
        if let Some(with) = with {
            not_implemented_if!(with.recursive);
            for cte in &with.cte_tables {
                if let Some(cte) = self.bind_cte(scope, cte)? {
                    ctes.push(cte);
                }
            }
        }

        let (scope, mut plan) = self.bind_table_expr(scope, body, order_by)?;

        // HACK: due to certain implementation details of how pipelines for the cte are currently built, this
        // is the right order to push the ctes in to allow the ctes to depend on each other in order.
        for cte in ctes.into_iter().rev() {
            plan = plan.with_cte(cte);
        }

        if let Some(limit) = limit {
            // LIMIT is currently not allowed to reference any columns
            let limit_expr = self.bind_expr(&Scope::default(), limit)?;
            if let Some(limit) = limit_expr
                .const_eval()
                .map_err(|EvalNotConst| anyhow!("LIMIT expression must be constant"))?
                .cast::<Option<u64>>()?
            {
                plan = plan.limit(limit);
            };
        }

        Ok((scope, plan))
    }

    fn bind_table_expr(
        &self,
        scope: &Scope,
        body: &ast::SetExpr,
        order_by: &[ast::OrderByExpr],
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, expr) = match body {
            ast::SetExpr::Select(sel) => self.bind_select(scope, sel, order_by)?,
            // FIXME we have to pass down order by into `bind_select` otherwise the projection
            // occurs before the order by which doesn't make sense (the order by won't have access to the required scope)
            // However, it also doesn't make sense to pass in the order_by for the following cases :/
            ast::SetExpr::Values(values) => {
                not_implemented_if!(!order_by.is_empty());
                let (scope, values) = self.bind_values(scope, values)?;
                (scope, ir::QueryPlan::values(values))
            }
            ast::SetExpr::Query(query) => self.bind_query(scope, query)?,
            ast::SetExpr::SetOperation { op, set_quantifier, left, right } => {
                not_implemented_if!(!order_by.is_empty());
                let (lhs_scope, lhs) = self.bind_table_expr(scope, left, &[])?;
                let (_rhs_scope, rhs) = self.bind_table_expr(scope, right, &[])?;
                let schema = if lhs.schema().is_subschema_of(rhs.schema()) {
                    rhs.schema().clone()
                } else if rhs.schema().is_subschema_of(lhs.schema()) {
                    lhs.schema().clone()
                } else {
                    bail!("schemas of set operation operands are not compatible")
                };

                let mut plan = match op {
                    ast::SetOperator::Union => lhs.union(schema, rhs),
                    // the others can be implemented as semi and anti joins
                    ast::SetOperator::Except => not_implemented!("except"),
                    ast::SetOperator::Intersect => not_implemented!("intersect"),
                };

                match set_quantifier {
                    ast::SetQuantifier::All => {}
                    ast::SetQuantifier::Distinct | ast::SetQuantifier::None => {
                        plan = plan.distinct()
                    }
                    ast::SetQuantifier::ByName => not_implemented!("by name"),
                    ast::SetQuantifier::AllByName => not_implemented!("all by name"),
                }

                // we just pick the left scope arbitrarily
                (lhs_scope, plan)
            }
            ast::SetExpr::Insert(stmt) => {
                not_implemented_if!(!order_by.is_empty());
                self.bind_insert(scope, stmt)?
            }
            ast::SetExpr::Update(stmt) => {
                not_implemented_if!(!order_by.is_empty());
                self.bind_update(scope, stmt)?
            }
            ast::SetExpr::Table(_) => {
                not_implemented_if!(!order_by.is_empty());
                todo!()
            }
        };

        Ok((scope, expr))
    }

    fn bind_create_sequence(&self, seq: &CreateSequenceInfo<'_>) -> Result<Box<ir::QueryPlan>> {
        // This is the plan to create the underlying sequence data table.
        let create_seq_table_plan = self.bind_create_table(
            seq.namespace,
            Name::clone(&seq.name),
            vec![
                CreateColumnInfo {
                    name: "key".into(),
                    ty: LogicalType::Oid,
                    identity: ColumnIdentity::None,
                    is_primary_key: true,
                    default_expr: None,
                },
                CreateColumnInfo {
                    name: "value".into(),
                    ty: LogicalType::Int64,
                    identity: ColumnIdentity::None,
                    is_primary_key: false,
                    default_expr: None,
                },
            ],
        )?;

        // This wraps the above plan and creates the corresponding entry in `nsql_sequence`.
        let create_seq_plan = Box::new(ir::QueryPlan::Insert {
            table: Table::SEQUENCE,
            source: ir::QueryPlan::project(
                create_seq_table_plan,
                [
                    ir::Expr::column_ref(
                        LogicalType::Oid,
                        QPath::new("", "oid"),
                        ir::TupleIndex::new(0),
                    ),
                    literal(seq.start),
                    literal(seq.step),
                ],
            ),
            returning: [ir::Expr::column_ref(
                LogicalType::Oid,
                QPath::new("t", "id"),
                ir::TupleIndex::new(0),
            )]
            .into(),
            schema: Schema::new([LogicalType::Oid]),
        });

        Ok(create_seq_plan)
    }

    fn bind_update(
        &self,
        scope: &Scope,
        stmt: &ast::Statement,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let ast::Statement::Update { table, assignments, from, selection, returning } = stmt else {
            panic!("expected insert statement")
        };

        not_implemented_if!(!table.joins.is_empty());
        not_implemented_if!(from.is_some());

        let (scope, table) = match &table.relation {
            ast::TableFactor::Table { name, alias, args, with_hints, version, partitions } => {
                not_implemented_if!(alias.is_some());
                not_implemented_if!(args.is_some());
                not_implemented_if!(!with_hints.is_empty());
                not_implemented_if!(version.is_some());
                not_implemented_if!(!partitions.is_empty());

                self.bind_base_table(scope, name, alias.as_ref())?
            }
            _ => not_implemented!("update with non-table relation"),
        };

        let mut source = self.build_table_scan(table, None)?;
        if let Some(predicate) = selection
            .as_ref()
            .map(|selection| self.bind_predicate(&scope, selection))
            .transpose()?
        {
            source = source.filter(predicate);
        }

        let assignments = self.bind_assignments(&scope, table, assignments)?;
        source = source.project(assignments);

        let returning = returning.as_ref().map_or(Ok([].into()), |items| {
            items
                .iter()
                .map(|selection| self.bind_select_item(&scope, selection))
                .flatten_ok()
                .collect::<Result<Box<_>>>()
        })?;

        let schema = returning.iter().map(|e| e.ty.clone()).collect::<Schema>();
        let scope = scope.project(&returning);

        Ok((scope, Box::new(ir::QueryPlan::Update { table, source, returning, schema })))
    }

    fn bind_create_table(
        &self,
        namespace: &Namespace,
        table_name: Name,
        columns: Vec<CreateColumnInfo>,
    ) -> Result<Box<ir::QueryPlan>> {
        if columns.iter().all(|c| !c.is_primary_key) {
            bail!("table must have a primary key defined")
        }

        let table_columns =
            self.catalog.get::<M, Table>(self.tx, Table::TABLE)?.columns(self.catalog, self.tx)?;
        assert_eq!(table_columns.len(), 3);
        let oid_column = &table_columns[0];

        // logically, the query for creating a new table is roughly as follows:
        // WITH t AS (INSERT INTO nsql_catalog.nsql_table VALUES (DEFAULT, 'namespace', 'name') RETURNING id)
        // INSERT INTO nsql_catalog.nsql_attribute
        // (SELECT t.id, 0 as index, name, .. FROM t UNION ALL col2... UNION ALL ...)
        let insert_table_plan = Box::new(ir::QueryPlan::Insert {
            table: Table::TABLE,
            source: ir::QueryPlan::values(ir::Values::new(
                [[
                    ir::Expr {
                        ty: LogicalType::Oid,
                        kind: ir::ExprKind::Compiled(oid_column.default_expr().clone()),
                    },
                    literal(namespace.oid()),
                    literal(Name::clone(&table_name)),
                ]
                .into()]
                .into(),
            )),
            returning: [ir::Expr::column_ref(
                LogicalType::Oid,
                ir::QPath::new("t", "id"),
                ir::TupleIndex::new(0),
            )]
            .into(),
            schema: Schema::new([LogicalType::Oid]),
        });

        // it's important that we generate a unique name for the cte since this query is sometimes nested
        let cte_name = Name::from(format!("{table_name}_cte"));

        let table_cte_scan = Box::new(ir::QueryPlan::CteScan {
            name: Name::clone(&cte_name),
            schema: Schema::new([LogicalType::Oid]),
        });

        Ok(Box::new(ir::QueryPlan::Insert {
            table: Table::ATTRIBUTE,
            // Return the oid of the table if the caller wants it for whatever reason.
            // Note there will be one row per inserted column
            returning: [
                ir::Expr::column_ref(
                    LogicalType::Oid,
                    ir::QPath::new("t", "id"),
                    ir::TupleIndex::new(0),
                                    )
            ]
            .into(),
            schema: Schema::new([LogicalType::Oid]),
            source: columns
                .into_iter()
                .enumerate()
                .map(|(index, column)| {
                    assert!(index < u8::MAX as usize);
                    if !matches!(column.identity, ColumnIdentity::None) {
                        ensure!(
                            column.ty == LogicalType::Int64,
                            "identity column must be of type int"
                        );
                    }

                    Ok(ir::QueryPlan::project(
                        table_cte_scan.clone(),
                        // order of these columns must match the order of the columns of the `Table` entity
                        [
                            ir::Expr::column_ref(
                                LogicalType::Oid,
                                ir::QPath::new(cte_name.as_str(), "id"),
                                ir::TupleIndex::new(0),
                            ),
                            literal(ir::Value::Byte(index as u8)),
                            literal(column.ty),
                            literal(Name::clone(&column.name)),
                            literal(column.is_primary_key),
                            literal(column.identity),
                            match column.identity {
                                ColumnIdentity::None => column
                                    .default_expr
                                    .map_or_else(|| literal(expr::Expr::null()), ir::Expr::quote),
                                ColumnIdentity::ByDefault | ColumnIdentity::Always => {
                                    ensure!(
                                        column.default_expr.is_none(),
                                        "both default and identity specified for column `{}` of table `{table_name}`",
                                        &column.name,
                                    );
                                    // If the column has a generated sequence, we need to create the sequence, it's underlying table and set the appropriate `default_expr` for the column.
                                    // The default value is computed roughly as the following pseudo-sql:
                                    // ```sql
                                    // WITH seq_oid AS (
                                    //    INSERT INTO nsql_catalog.nsql_sequence (oid)
                                    //    SELECT (INSERT INTO nsql_catalog.nsql_table VALUES (DEFAULT, 'namespace', 'name') RETURNING id
                                    // ) NEXTVAL(seq_oid)
                                    // ```
                                    // The subtlety here is that the CTE evaluates to the expression `NEXTVAL(seq_oid)`,
                                    // which is then evaluated only when a row is inserted into the table.
                                    // Again, it evaluates to an expression (as expressions are values in nsql).

                                    let name = Name::from(format!(
                                        "{}_{table_name}_{}_seq",
                                        namespace.name(),
                                        &column.name
                                    ));

                                    let create_seq_plan = self.bind_create_sequence( &CreateSequenceInfo { namespace, name, start: 1, step: 1 }, )?;

                                    // The generated `CREATE SEQUENCE` query above returns the oid of the sequence which is exactly what we need here.
                                    // We reference the new sequence oid in a subquery expr to construct the `default_expr` value.
                                    // This `expr` is then passed to the `MK_NEXTVAL_EXPR` which takes the `seq_oid` and creates an expr that evaluates `NEXTVAL(seq_oid)`
                                    // which is precisely what the `default_expr` should be.
                                    let sequence_oid_expr =
                                        ir::Expr::scalar_subquery(create_seq_plan)?;
                                    assert_eq!(sequence_oid_expr.ty, LogicalType::Oid);

                                    let f = self
                                        .catalog
                                        .functions(self.tx)?
                                        .get(Function::MK_NEXTVAL_EXPR)?;

                                    let function = ir::MonoFunction::new(f, LogicalType::Expr);
                                    ir::Expr::call(function, [sequence_oid_expr])
                                }
                            },
                        ],
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?
                .into_iter()
                .reduce(|a, b| {
                    assert_eq!(a.schema(), b.schema());
                    let schema = a.schema().clone();
                    a.union(schema, b)
                })
                .ok_or_else(|| anyhow!("table must define at least one column"))?,
        })
        // The generated plan returns `n` rows with the oid of the table so we pick the first.
        .ungrouped_aggregate([(
            ir::MonoFunction::new(ir::Function::first(), LogicalType::Oid),
            [ir::Expr::column_ref(
                LogicalType::Oid,
                ir::QPath::new("t", "oid"),
                ir::TupleIndex::new(0),
            )]
            .into(),
        )])
        .with_cte(ir::Cte { name: Name::clone(&cte_name), plan: insert_table_plan }))
    }

    fn bind_insert(
        &self,
        scope: &Scope,
        stmt: &ast::Statement,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let ast::Statement::Insert {
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
        } = stmt
        else {
            panic!("expected insert statement")
        };

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

        let (_, mut source) = self.bind_query(scope, source)?;

        let (scope, table) = self.bind_base_table(scope, table_name, None)?;

        let table_columns =
            self.catalog.table::<M>(self.tx, table)?.columns(self.catalog, self.tx)?;

        if source.schema().width() > table_columns.len() {
            bail!(
                "table `{}` has {} columns but {} columns were supplied",
                table_name,
                table_columns.len(),
                source.schema().width()
            )
        }

        // if there are no columns specified, then we apply the given `k` columns in order and fill the right with the default values
        let projection = if columns.is_empty() {
            table_columns
                .iter()
                .enumerate()
                .map(|(i, column)| {
                    let ty = column.logical_type().clone();
                    if i < source.schema().width() {
                        ir::Expr::column_ref(
                            ty,
                            QPath::new(table.to_string().as_ref(), column.name()),
                            TupleIndex::new(i),
                        )
                    } else {
                        ir::Expr { ty, kind: ir::ExprKind::Compiled(column.default_expr().clone()) }
                    }
                })
                .collect()
        } else {
            // If insert columns are proivded, then we model the insertion columns list as a projection over the source, with missing columns filled in with defaults
            let target_column_indices = columns
                .iter()
                .map(|ident| ast::Expr::Identifier(ident.clone()))
                .map(|expr| match self.bind_expr(&scope, &expr)?.kind {
                    ir::ExprKind::ColumnRef(ir::ColumnRef { index, .. }) => Ok(index),
                    _ => unreachable!("expected column reference"),
                })
                .collect::<Result<Vec<_>>>()?;

            // loop over all columns in the table and find the corresponding column in the base projection,
            // if one does not exist then we fill it in with a null
            let source_schema = source.schema();
            table_columns
                .iter()
                .map(|column| {
                    if let Some(expr) =
                        target_column_indices.iter().enumerate().find_map(|(i, column_index)| {
                            (column_index.as_usize() == column.index().as_usize()).then_some(
                                ir::Expr::column_ref(
                                    source_schema[i].clone(),
                                    QPath::new(table.to_string().as_ref(), column.name()),
                                    TupleIndex::new(i),
                                ),
                            )
                        })
                    {
                        expr
                    } else {
                        let ty = column.logical_type().clone();
                        ir::Expr { ty, kind: ir::ExprKind::Compiled(column.default_expr().clone()) }
                    }
                })
                .collect_vec()
        };

        assert_eq!(projection.len(), table_columns.len());
        source = source.project(projection);

        let source_schema = source.schema();
        assert_eq!(source_schema.width(), table_columns.len());

        for (column, ty) in table_columns.iter().zip(source_schema) {
            ensure!(
                ty.is_subtype_of(&column.logical_type()),
                "cannot insert value of type `{ty}` into column `{}` of type `{}`",
                column.name(),
                column.logical_type()
            )
        }

        let returning = returning.as_ref().map_or(Ok([].into()), |items| {
            items
                .iter()
                .map(|selection| self.bind_select_item(&scope, selection))
                .flatten_ok()
                .collect::<Result<Box<_>>>()
        })?;

        let schema = returning.iter().map(|e| e.ty.clone()).collect::<Schema>();
        let scope = scope.project(&returning);

        Ok((scope, Box::new(ir::QueryPlan::Insert { table, source, returning, schema })))
    }

    fn bind_select(
        &self,
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
        not_implemented_if!(top.is_some());
        not_implemented_if!(into.is_some());
        not_implemented_if!(!lateral_views.is_empty());
        not_implemented_if!(!cluster_by.is_empty());
        not_implemented_if!(!distribute_by.is_empty());
        not_implemented_if!(!sort_by.is_empty());
        not_implemented_if!(qualify.is_some());

        let (scope, mut source) = match &from[..] {
            [] => (scope.clone(), Box::new(ir::QueryPlan::DummyScan)),
            [table] => self.bind_joint_tables(scope, table)?,
            _ => todo!(),
        };

        if let Some(predicate) = selection {
            let predicate = self.bind_predicate(&scope, predicate)?;
            source = source.filter(predicate);
        }

        let group_by = match group_by {
            ast::GroupByExpr::All => not_implemented!("group by all"),
            ast::GroupByExpr::Expressions(group_by) => group_by
                .iter()
                .map(|expr| self.bind_expr(&scope, expr))
                .collect::<Result<Box<_>>>()?,
        };

        let (scope, mut plan) = SelectBinder::new(self, group_by).bind(
            &scope,
            source,
            projection,
            order_by,
            having.as_ref(),
        )?;

        if let Some(distinct) = distinct {
            plan = match distinct {
                ast::Distinct::Distinct => plan.distinct(),
                ast::Distinct::On(_) => not_implemented!("distinct on"),
            }
        }

        Ok((scope, plan))
    }

    fn bind_joint_tables(
        &self,
        scope: &Scope,
        tables: &ast::TableWithJoins,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (mut scope, mut plan) = self.bind_table_factor(scope, &tables.relation)?;

        for join in &tables.joins {
            (scope, plan) = self.bind_join(&scope, plan, join)?;
        }

        Ok((scope, plan))
    }

    fn bind_join(
        &self,
        lhs_scope: &Scope,
        lhs: Box<ir::QueryPlan>,
        join: &ast::Join,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (rhs_scope, rhs) = self.bind_table_factor(&Scope::default(), &join.relation)?;

        let scope = lhs_scope.join(&rhs_scope);
        let kind = match join.join_operator {
            ast::JoinOperator::Inner(_) | ast::JoinOperator::CrossJoin => ir::JoinKind::Inner,
            ast::JoinOperator::LeftOuter(_) => ir::JoinKind::Left,
            ast::JoinOperator::RightOuter(_) => ir::JoinKind::Right,
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
                    let predicate = self.bind_predicate(&scope, predicate)?;
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
        scope: &Scope,
        table: &ast::TableFactor,
    ) -> Result<(Scope, Box<ir::QueryPlan>)> {
        let (scope, table_expr) = match table {
            ast::TableFactor::Table { name, alias, args, with_hints, version, partitions } => {
                not_implemented_if!(args.is_some());
                not_implemented_if!(!with_hints.is_empty());
                not_implemented_if!(version.is_some());
                not_implemented_if!(!partitions.is_empty());

                let path = self.lower_path(&name.0)?;
                let (scope, table) = self.bind_table(scope, &path, alias.as_ref())?;
                let plan = match table {
                    TableBinding::MaterializedCte(name, schema) => {
                        ir::QueryPlan::cte_scan(name, schema)
                    }
                    TableBinding::InlineCte(plan) => plan,
                    TableBinding::Table(table) => self.build_table_scan(table, None)?,
                };

                (scope, plan)
            }
            ast::TableFactor::Derived { lateral, subquery, alias } => {
                not_implemented_if!(*lateral);

                let (mut scope, subquery) = self.bind_subquery(scope, subquery)?;
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

                let expr = self.bind_expr(scope, &array_exprs[0])?;

                let mut scope = scope.bind_unnest(&expr)?;

                if let Some(alias) = alias {
                    scope = scope.alias(self.lower_table_alias(alias))?;
                }

                (scope, ir::QueryPlan::unnest(expr))
            }
            ast::TableFactor::NestedJoin { .. } => not_implemented!("nested join"),
            ast::TableFactor::Pivot { .. } => not_implemented!("pivot"),
            ast::TableFactor::Unpivot { .. } => not_implemented!("unpivot"),
        };

        Ok((scope, table_expr))
    }

    fn bind_table(
        &self,
        scope: &Scope,
        path: &Path,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Scope, TableBinding)> {
        let alias = alias.map(|alias| self.lower_table_alias(alias));
        scope.bind_table(self, path, alias.as_ref())
    }

    fn bind_base_table(
        &self,
        scope: &Scope,
        table_name: &ast::ObjectName,
        alias: Option<&ast::TableAlias>,
    ) -> Result<(Scope, Oid<Table>)> {
        let alias = alias.map(|alias| self.lower_table_alias(alias));
        let path = self.lower_path(&table_name.0)?;
        scope.bind_base_table(self, &path, alias.as_ref())
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
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                self.bind_expr(scope, expr)?.alias(&alias.value)
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
                    .map(|ident| self.bind_ident(scope, ident).map(|(_ty, col)| col.index))
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
                .collect::<Result<Box<_>, _>>()?,
        );

        Ok((scope.bind_values(&values), values))
    }

    fn bind_row(&self, scope: &Scope, row: &[ast::Expr]) -> Result<Box<[ir::Expr]>> {
        row.iter().map(|expr| self.bind_expr(scope, expr)).collect()
    }

    fn bind_predicate(&self, scope: &Scope, predicate: &ast::Expr) -> Result<ir::Expr> {
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
        scope: &Scope,
        ident: &ast::Ident,
    ) -> Result<(LogicalType, ir::ColumnRef)> {
        scope.lookup_column(&Path::Unqualified(ident.value.clone().into()))
    }

    fn bind_function(&self, scope: &Scope, f: &ast::Function) -> Result<ir::Expr> {
        let ast::Function { name, args, over, distinct, special, order_by } = f;
        not_implemented_if!(over.is_some());
        not_implemented_if!(*distinct);
        not_implemented_if!(*special);
        not_implemented_if!(!order_by.is_empty());

        if f.name.0.len() == 1 && f.name.0[0].value.to_lowercase() == "coalesce" {
            let ast::Function { name: _, args, over, distinct, special, order_by } = f;
            // should probably give a more specific error for coalesce
            not_implemented_if!(over.is_some());
            not_implemented_if!(*distinct);
            not_implemented_if!(*special);
            not_implemented_if!(!order_by.is_empty());

            ensure!(!args.is_empty(), "coalesce requires at least one argument");

            let args = self.bind_args(scope, args)?;
            let ty = self
                .ensure_exprs_have_compat_types(&args)
                .map_err(|(expected, actual)| {
                    anyhow!(
                        "`coalesce` arguments must have compatible types: expected type {expected}, got {actual}",
                    )
                })?
                // if all arguments are `NULL` then default to the `NULL` type
                .unwrap_or(LogicalType::Null);
            return Ok(ir::Expr { ty, kind: ir::ExprKind::Coalesce(args) });
        }

        let args = self.bind_args(scope, args)?;
        let arg_types = args.iter().map(|arg| arg.ty.clone()).collect::<Box<_>>();
        let path = self.lower_path(&name.0)?;
        let function = self.resolve_function(&path, &arg_types)?;

        Ok(ir::Expr {
            ty: function.return_type(),
            kind: ir::ExprKind::FunctionCall { function, args },
        })
    }

    fn bind_args(&self, scope: &Scope, args: &[ast::FunctionArg]) -> Result<Box<[ir::Expr]>> {
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

        args.iter().map(|arg| self.bind_expr(scope, arg)).collect::<Result<Box<_>, _>>()
    }

    fn bind_unary_operator(
        &self,
        name: &'static str,
        operand: LogicalType,
    ) -> Result<ir::MonoOperator> {
        self.bind_operator(name, OperatorKind::Unary, LogicalType::Null, operand)
    }

    fn bind_binary_operator(
        &self,
        name: &'static str,
        left: LogicalType,
        right: LogicalType,
    ) -> Result<ir::MonoOperator> {
        self.bind_operator(name, OperatorKind::Binary, left, right)
    }

    fn bind_operator(
        &self,
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

        let (operators, namespace, name) =
            self.get_namespaced_entity_view::<Operator>(&Path::qualified("nsql_catalog", name))?;

        let mut candidates = operators
            .as_ref()
            .scan(..)?
            .filter(|op| {
                Ok(op.parent_oid(self.catalog, self.tx)?.unwrap() == namespace.key()
                    && op.name() == name)
            })
            .peekable();

        if candidates.peek()?.is_none() {
            return Err(unbound!(Operator, name));
        }

        let operator = match kind {
            OperatorKind::Unary => {
                let args = [right];
                self.resolve_candidate_operators(candidates, &args)?.ok_or_else(|| {
                    let [right] = args;
                    anyhow!("no operator overload for `{name}{right}`")
                })?
            }
            OperatorKind::Binary => {
                let args = [left, right];
                self.resolve_candidate_operators(candidates, &args)?.ok_or_else(|| {
                    let [left, right] = args;
                    anyhow!("no operator overload for `{left} {name} {right}`")
                })?
            }
        };

        Ok(operator)
    }

    fn resolve_function(&self, path: &Path, args: &[LogicalType]) -> Result<ir::MonoFunction> {
        let (functions, namespace, name) = self.get_namespaced_entity_view::<Function>(path)?;
        let mut candidates = functions
            .as_ref()
            .scan(..)?
            .filter(|f| {
                Ok(f.parent_oid(self.catalog, self.tx)?.unwrap() == namespace.key()
                    && f.name() == name)
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
        scope: &Scope,
        expr: &ast::Expr,
        mut f: impl FnMut(&ast::Expr) -> Result<ir::Expr>,
    ) -> Result<ir::Expr> {
        let (ty, kind) = match expr {
            ast::Expr::Value(literal) => return Ok(self.bind_value_expr(literal)),
            ast::Expr::Nested(expr) => return self.walk_expr(scope, expr, f),
            ast::Expr::Identifier(ident) => {
                let (ty, col) = self.bind_ident(scope, ident)?;
                (ty, ir::ExprKind::ColumnRef(col))
            }
            ast::Expr::CompoundIdentifier(ident) => {
                let path = self.lower_path(ident)?;
                let (ty, col) = scope.lookup_column(&path)?;
                (ty, ir::ExprKind::ColumnRef(col))
            }
            ast::Expr::UnaryOp { op, expr } => return self.bind_unary_expr(op, expr, &mut f),
            ast::Expr::BinaryOp { left, op, right } => {
                let op = match op {
                    ast::BinaryOperator::Eq => Operator::EQ,
                    ast::BinaryOperator::NotEq => Operator::NEQ,
                    ast::BinaryOperator::Plus => Operator::PLUS,
                    ast::BinaryOperator::Minus => Operator::MINUS,
                    ast::BinaryOperator::Lt => Operator::LT,
                    ast::BinaryOperator::LtEq => Operator::LTE,
                    ast::BinaryOperator::GtEq => Operator::GTE,
                    ast::BinaryOperator::Gt => Operator::GT,
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
                return self.bind_binary_expr(op, left, right, &mut f);
            }
            ast::Expr::IsDistinctFrom(lhs, rhs) => {
                return self.bind_binary_expr(Operator::IS_DISTINCT_FROM, lhs, rhs, f);
            }
            ast::Expr::IsNotDistinctFrom(lhs, rhs) => {
                return self.bind_binary_expr(Operator::IS_NOT_DISTINCT_FROM, lhs, rhs, f);
            }
            // Implement `x IS NULL` as `x IS NOT DISTINCT FROM NULL` for now although this is not quite spec compliant.
            // https://stackoverflow.com/questions/58997907/is-there-a-difference-between-is-null-and-is-not-distinct-from-null
            ast::Expr::IsNull(expr) => {
                let expr = Box::new(f(expr)?);
                let operator = self.bind_binary_operator(
                    Operator::IS_NOT_DISTINCT_FROM,
                    expr.ty(),
                    expr.ty(),
                )?;
                let ty = expr.ty();
                (
                    operator.return_type(),
                    ir::ExprKind::BinaryOperator {
                        operator,
                        lhs: expr,
                        rhs: Box::new(ir::Expr {
                            ty,
                            kind: ir::ExprKind::Literal(ir::Value::Null),
                        }),
                    },
                )
            }
            ast::Expr::IsNotNull(expr) => {
                let expr = Box::new(f(expr)?);
                let operator =
                    self.bind_binary_operator(Operator::IS_DISTINCT_FROM, expr.ty(), expr.ty())?;
                let ty = expr.ty();
                (
                    operator.return_type(),
                    ir::ExprKind::BinaryOperator {
                        operator,
                        lhs: expr,
                        rhs: Box::new(ir::Expr {
                            ty,
                            kind: ir::ExprKind::Literal(ir::Value::Null),
                        }),
                    },
                )
            }
            ast::Expr::Between { expr, negated, low, high } => {
                if *negated {
                    return self.walk_expr(
                        scope,
                        &ast::Expr::UnaryOp {
                            op: ast::UnaryOperator::Not,
                            expr: Box::new(ast::Expr::Between {
                                expr: expr.clone(),
                                negated: false,
                                low: low.clone(),
                                high: high.clone(),
                            }),
                        },
                        f,
                    );
                }

                // implement `BETWEEN` as it's own function rather than desugaring to `x >= low AND x <= high` for potential efficiency gains
                let expr = f(expr)?;
                let low = f(low)?;
                let high = f(high)?;
                let args = [expr.ty(), low.ty(), high.ty()];
                let function =
                    self.resolve_function(&Path::qualified(MAIN_SCHEMA_PATH, "between"), &args)?;
                (
                    function.return_type(),
                    ir::ExprKind::FunctionCall { function, args: [expr, low, high].into() },
                )
            }

            ast::Expr::Cast { expr, data_type } => {
                let expr = f(expr)?;
                let target = self.lower_ty(data_type)?;
                // We implement `CAST` by hacking operator overloading.
                // We search for a cast function `(from, target) -> target` with the second parameter being a dummy argument.
                let args = [expr.ty(), target];
                let function =
                    self.resolve_function(&Path::qualified(MAIN_SCHEMA_PATH, "cast"), &args)?;
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
                let exprs = array.elem.iter().map(f).collect::<Result<Box<_>, _>>()?;
                let ty =
                    self.ensure_exprs_have_compat_types(&exprs).map_err(|(actual, expected)| {
                        anyhow!(
                            "cannot create array of type {expected} with element of type {actual}",
                        )
                    })?;

                // default type to `Int64` if there are no non-null elements
                let ty = ty.unwrap_or(LogicalType::Int64);
                (LogicalType::Array(Box::new(ty)), ir::ExprKind::Array(exprs))
            }
            ast::Expr::Function(f) => {
                let expr = self.bind_function(scope, f)?;
                match expr.kind {
                    ir::ExprKind::FunctionCall { function, .. }
                        if matches!(function.kind(), FunctionKind::Aggregate) =>
                    {
                        // fixme better message
                        bail!("aggregate function not allowed here")
                    }
                    kind => (expr.ty, kind),
                }
            }
            ast::Expr::Subquery(subquery) => {
                let (_, plan) = self.bind_subquery(scope, subquery)?;
                return ir::Expr::scalar_subquery(plan);
            }
            ast::Expr::Exists { subquery, negated } => {
                // `NOT EXISTS (subquery)` is literally `NOT (EXISTS (subquery))` so do that transformation
                if *negated {
                    return self.walk_expr(
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

                let (_scope, plan) = self.bind_subquery(scope, subquery)?;
                (LogicalType::Bool, ir::ExprKind::Subquery(ir::SubqueryKind::Exists, plan))
            }
            ast::Expr::InList { expr, list, negated } => {
                assert!(!list.is_empty(), "this should be syntactically invalid");
                // x IN (x1..xn) is desugared into `x = x1 OR .. OR x = xn`. This maintains the desired NULL behaviour.
                // x NOT IN (x1..xn) => `x <> x1 AND .. AND x <> xn`
                // https://www.postgresql.org/docs/current/functions-comparisons.html#FUNCTIONS-COMPARISONS-NOT-IN

                let op =
                    if *negated { ast::BinaryOperator::NotEq } else { ast::BinaryOperator::Eq };

                let desugared = list.iter().skip(1).cloned().fold(
                    ast::Expr::BinaryOp {
                        left: expr.clone(),
                        op,
                        right: Box::new(list[0].clone()),
                    },
                    |acc, x| ast::Expr::BinaryOp {
                        left: Box::new(acc),
                        op: if *negated {
                            ast::BinaryOperator::And
                        } else {
                            ast::BinaryOperator::Or
                        },
                        right: Box::new(ast::Expr::BinaryOp {
                            left: expr.clone(),
                            op: if *negated {
                                ast::BinaryOperator::NotEq
                            } else {
                                ast::BinaryOperator::Eq
                            },
                            right: Box::new(x),
                        }),
                    },
                );

                let expr = self.walk_expr(scope, &desugared, f)?;
                assert_eq!(
                    expr.ty,
                    LogicalType::Bool,
                    "expected desugared IN expr to evaluate to bool ({expr})"
                );

                return Ok(expr);
            }
            ast::Expr::Case { operand, conditions, results, else_result } => {
                assert_eq!(conditions.len(), results.len());
                assert!(!conditions.is_empty());

                let scrutinee = match operand {
                    Some(scrutinee) => f(scrutinee)?,
                    // if there is no scrutinee, we can just use a literal `true` to compare each branch against
                    None => literal(true),
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

                let mut result_type: LogicalType = cases[0].then.ty.clone();
                for expr in
                    cases.iter().map(|case| &case.then).chain(else_result.iter().map(AsRef::as_ref))
                {
                    let expr: &ir::Expr = expr;
                    result_type = result_type.common_supertype(&expr.ty).ok_or_else(|| {
                        anyhow!(
                            "all case results must have the same type, expected `{}`, got `{}`",
                            result_type,
                            expr.ty
                        )
                    })?;
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

    fn bind_unary_expr(
        &self,
        op: &ast::UnaryOperator,
        expr: &ast::Expr,
        mut f: impl FnMut(&ast::Expr) -> Result<ir::Expr>,
    ) -> Result<ir::Expr> {
        let bound_expr = f(expr)?;
        let expr = Box::new(bound_expr);

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

        let operator = self.bind_unary_operator(op, expr.ty())?;
        let ty = operator.return_type();
        let kind = ir::ExprKind::UnaryOperator { operator, expr };

        Ok(ir::Expr { ty, kind })
    }

    fn bind_binary_expr(
        &self,
        op: &'static str,
        lhs: &ast::Expr,
        rhs: &ast::Expr,
        mut f: impl FnMut(&ast::Expr) -> Result<ir::Expr>,
    ) -> Result<ir::Expr> {
        let lhs = Box::new(f(lhs)?);
        let rhs = Box::new(f(rhs)?);

        let operator = self.bind_binary_operator(op, lhs.ty(), rhs.ty())?;
        let ty = operator.return_type();
        let kind = ir::ExprKind::BinaryOperator { operator, lhs, rhs };

        Ok(ir::Expr { ty, kind })
    }

    // Ensure that all expressions have compatible types.
    fn ensure_exprs_have_compat_types(
        &self,
        exprs: &[ir::Expr],
    ) -> Result<Option<LogicalType>, (LogicalType, LogicalType)> {
        let mut expected_ty = None;
        for expr in exprs {
            if let Some(expected) = &expected_ty {
                if !expr.ty.is_subtype_of(expected) {
                    return Err((expected.clone(), expr.ty.clone()));
                }
            } else if !expr.ty.is_null() {
                expected_ty = Some(expr.ty.clone());
            }
        }

        Ok(expected_ty)
    }

    fn bind_expr(&self, scope: &Scope, expr: &ast::Expr) -> Result<ir::Expr> {
        self.walk_expr(scope, expr, |expr| self.bind_expr(scope, expr))
    }

    fn bind_value_expr(&self, value: &ast::Value) -> ir::Expr {
        let value = self.bind_value(value);
        literal(value)
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

    /// bind a cte, returning `Some(cte)` if the cte is materialized, and none otherwise
    fn bind_cte(&self, scope: &Scope, cte: &ast::Cte) -> Result<Option<ir::Cte>> {
        let ast::Cte { alias, query, from, materialized } = cte;
        let materialized = materialized.unwrap_or(true);
        let kind = if materialized { CteKind::Materialized } else { CteKind::Inline };

        // not sure what the `from` is even about for a cte
        not_implemented_if!(from.is_some());
        let alias = self.lower_table_alias(alias);
        let (cte_scope, cte_plan) = self.bind_query(scope, query)?;
        let cte = ir::Cte { name: Name::clone(&alias.table_name), plan: cte_plan.clone() };

        let name = Name::clone(&alias.table_name);
        let scope = cte_scope.alias(alias)?;

        match self.ctes.borrow_mut().entry(Name::clone(&name)) {
            Entry::Occupied(_) => {
                bail!("common table expression with name `{name}` specified more than once")
            }
            Entry::Vacant(entry) => {
                entry.insert((kind, scope, cte_plan));
            }
        }

        match kind {
            CteKind::Inline => Ok(None),
            CteKind::Materialized => Ok(Some(cte)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableAlias {
    table_name: Name,
    columns: Vec<Name>,
}

fn literal(value: impl Into<ir::Value>) -> ir::Expr {
    // resist moving this logic out of this binder module.
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
                    .map(default_logical_type_of_value)
                    .unwrap_or(LogicalType::Int64),
            ),
            ir::Value::Type(_) => LogicalType::Type,
            ir::Value::Expr(_) => LogicalType::Expr,
            ir::Value::TupleExpr(_) => LogicalType::TupleExpr,
            ir::Value::Byte(_) => LogicalType::Byte,
            ir::Value::Float64(_) => LogicalType::Float64,
        }
    }
    let value = value.into();
    let ty = default_logical_type_of_value(&value);
    ir::Expr { kind: ir::ExprKind::Literal(value), ty }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CreateColumnInfo {
    pub name: Name,
    pub ty: LogicalType,
    pub is_primary_key: bool,
    pub identity: ColumnIdentity,
    pub default_expr: Option<ir::Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CreateSequenceInfo<'a> {
    namespace: &'a Namespace,
    name: Name,
    start: i64,
    step: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrimaryKeyConstraint {
    column_indices: Vec<usize>,
}
