use egg::{define_language, Id};
use ir::Value;
use nsql_core::{LogicalType, Oid};
use rustc_hash::FxHashMap;

use crate::Query;

pub(crate) type EGraph = egg::EGraph<Node, ()>;

define_language! {
    /// The logical optimizer specific language for rule based optimization.
    // This should be crate-private
    pub(crate) enum Node {
        // scalar expressions
        Literal(Value),
        ColumnRef(ir::TupleIndex),
        "array" = Array(Box<[Id]>),
        "subquery" = Subquery(Id),
        "case" = Case([Id; 3]), // (case <scrutinee> (<condition> <then> <condition> <then> ...) <else>)

        // a "list" of nodes
        "nodes" = Nodes(Box<[Id]>),

        // relational expression
        "dummy" = DummyScan,
        "project" = Project([Id; 2]),            // (project <source> (<exprs> ...))
        "filter" = Filter([Id; 2]),              // (filter <source> <predicate>)
        "join" = Join([Id; 3]),                  // (join <join-expr> <lhs> <rhs>)
        "cross-join" = CrossJoin([Id; 2]),       // (cross-join <lhs> <rhs>)
        "unnest" = Unnest(Id),                   // (unnest <array-expr>)
        "order" = Order([Id; 2]),                // (order <source> (<order-exprs>...))
        "limit" = Limit([Id; 2]),                // (limit <source> <limit>)
        "scan" = TableScan(Id),                  // (scan <table>)
        "agg" = Aggregate([Id; 3]),              // (agg <source> <group_by>... <aggregates-calls>...)
        "strict_limit" = StrictLimit([Id; 3]),   // (strict_limit <source> <limit> <message>)
        "values" = Values(Box<[Id]>),

        "insert" = Insert([Id; 3]),    // (insert <table> <source> <returning>)
        "update" = Update([Id; 3]),    // (update <table> <source> <returning>)

        // pseudo expressions
        "desc" = Desc(Id),
        JoinOn(ir::JoinKind, Id),
        Table(Oid<ir::Table>),
        Function(Oid<ir::Function>),
        "call" = Call([Id; 2]), // (call f (args...))
    }
}

#[derive(Debug, Default)]
pub(crate) struct Builder {
    egraph: EGraph,
}

impl Builder {
    pub fn build(mut self, query: &ir::QueryPlan) -> Query {
        let root = self.build_query(query);
        Query::new(self.egraph, root)
    }

    fn build_query(&mut self, query: &ir::QueryPlan) -> Id {
        let expr = match query {
            ir::QueryPlan::DummyScan => Node::DummyScan,
            ir::QueryPlan::Aggregate { aggregates, source, group_by, schema: _ } => {
                let source = self.build_query(source);
                let group_by = self.build_exprs(group_by);
                let functions = Node::Nodes(
                    aggregates
                        .iter()
                        .map(|(f, args)| {
                            let f = self.add(Node::Function(f.oid()));
                            let args = self.build_exprs(args);
                            self.add(Node::Call([f, args]))
                        })
                        .collect(),
                );
                Node::Aggregate([source, group_by, self.add(functions)])
            }
            ir::QueryPlan::TableScan { table, projection: None, projected_schema: _ } => {
                Node::TableScan(self.add(Node::Table(*table)))
            }
            ir::QueryPlan::TableScan { .. } => todo!(),
            ir::QueryPlan::Projection { source, projection, projected_schema: _ } => {
                Node::Project([self.build_query(source), self.build_exprs(projection)])
            }
            ir::QueryPlan::Filter { source, predicate } => {
                Node::Filter([self.build_query(source), self.build_expr(predicate)])
            }
            ir::QueryPlan::Unnest { schema: _, expr } => Node::Unnest(self.build_expr(expr)),
            ir::QueryPlan::Values { values, schema: _ } => {
                Node::Values(values.iter().map(|exprs| self.build_exprs(exprs)).collect())
            }
            ir::QueryPlan::Join { schema: _, join, lhs, rhs } => {
                let lhs = self.build_query(lhs);
                let rhs = self.build_query(rhs);
                match join {
                    ir::Join::Cross => Node::CrossJoin([lhs, rhs]),
                    ir::Join::Constrained(kind, constraint) => {
                        let constraint = match constraint {
                            ir::JoinConstraint::On(expr) => {
                                Node::JoinOn(*kind, self.build_expr(expr))
                            }
                        };
                        Node::Join([self.egraph.add(constraint), lhs, rhs])
                    }
                }
            }
            ir::QueryPlan::Limit { source, limit, exceeded_message } => {
                let source = self.build_query(source);
                let limit = self.add_value(Value::Int64(*limit as i64));
                match exceeded_message {
                    Some(message) => Node::StrictLimit([
                        source,
                        limit,
                        self.add_value(Value::Text(message.to_string())),
                    ]),
                    None => Node::Limit([source, limit]),
                }
            }
            ir::QueryPlan::Order { source, order } => {
                let source = self.build_query(source);
                let order_exprs = order.iter().map(|expr| self.build_order_expr(expr)).collect();
                let order = self.add(Node::Nodes(order_exprs));
                Node::Order([source, order])
            }
            ir::QueryPlan::Insert { table, source, returning, schema: _ } => {
                let source = self.build_query(source);
                let returning = self.build_exprs(returning.as_deref().unwrap_or(&[]));
                let table = self.add(Node::Table(*table));
                Node::Insert([table, source, returning])
            }
            ir::QueryPlan::Update { table, source, returning, schema: _ } => {
                let source = self.build_query(source);
                let returning = self.build_exprs(returning.as_deref().unwrap_or(&[]));
                let table = self.add(Node::Table(*table));
                Node::Update([table, source, returning])
            }
        };

        self.add(expr)
    }

    fn build_order_expr(&mut self, order_expr: &ir::OrderExpr) -> Id {
        let id = self.build_expr(&order_expr.expr);
        if order_expr.asc { id } else { self.add(Node::Desc(id)) }
    }

    fn add_value(&mut self, value: Value) -> Id {
        self.add(Node::Literal(value))
    }

    fn add(&mut self, expr: Node) -> Id {
        self.egraph.add(expr)
    }

    fn build_exprs(&mut self, exprs: &[ir::Expr]) -> Id {
        let id = Node::Nodes(exprs.iter().map(|expr| self.build_expr(expr)).collect());
        self.add(id)
    }

    fn build_expr(&mut self, expr: &ir::Expr) -> Id {
        let expr = match &expr.kind {
            ir::ExprKind::Literal(value) => Node::Literal(value.clone()),
            ir::ExprKind::Array(exprs) => {
                Node::Array(exprs.iter().map(|expr| self.build_expr(expr)).collect())
            }
            ir::ExprKind::Alias { alias: _, expr } => return self.build_expr(expr),
            ir::ExprKind::ColumnRef { qpath: _, index } => Node::ColumnRef(*index),
            ir::ExprKind::FunctionCall { function, args } => {
                let f = self.add(Node::Function(function.oid()));
                let args = self.build_exprs(args);
                Node::Call([f, args])
            }
            ir::ExprKind::UnaryOperator { operator, expr } => {
                let f = self.add(Node::Function(operator.mono_function().oid()));
                let expr = self.build_expr(expr);
                Node::Call([f, self.add(Node::Nodes([expr].into()))])
            }
            ir::ExprKind::BinaryOperator { operator, lhs, rhs } => {
                let f = self.add(Node::Function(operator.mono_function().oid()));
                let lhs = self.build_expr(lhs);
                let rhs = self.build_expr(rhs);
                Node::Call([f, self.add(Node::Nodes([lhs, rhs].into()))])
            }
            ir::ExprKind::Case { scrutinee, cases, else_result } => {
                let scrutinee = self.build_expr(scrutinee);
                let cases = cases
                    .iter()
                    .flat_map(|case| {
                        let when = self.build_expr(&case.when);
                        let then = self.build_expr(&case.then);
                        [when, then]
                    })
                    .collect();

                let else_result =
                    self.build_expr(else_result.as_deref().unwrap_or(&ir::Expr::NULL));

                Node::Case([scrutinee, self.add(Node::Nodes(cases)), else_result])
            }
            ir::ExprKind::Subquery(_kind, query) => Node::Subquery(self.build_query(query)),
        };

        self.add(expr)
    }
}
