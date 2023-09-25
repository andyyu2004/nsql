use egg::{define_language, Id};
use ir::Value;
use nsql_core::{Name, Oid};
use nsql_storage::eval;

use crate::Query;

pub(crate) type EGraph = egg::EGraph<Node, ()>;

define_language! {
    /// The logical optimizer specific language for rule based optimization.
    // This should be crate-private. The `view` module exposes a typed view into these raw nodes.
    pub(crate) enum Node {
        // scalar expressions
        Literal(Value),
        Cte(Name, [Id; 2]), // (cte-name (cte-plan child-plan))
        CteScan(Name),
        CompiledExpr(eval::Expr),
        // We pass the plan id here so column refs with the same index don't get merged into the same eclass
        ColumnRef(ir::ColumnRef, Id),           // (column-ref <index> <plan>)
        "union" = Union([Id; 2]),               // (union <lhs> <rhs>)
        "array" = Array(Box<[Id]>),
        "distinct" = Distinct(Id),
        "quote" = QuotedExpr(Id),
        "subquery" = Subquery(Id),
        "exists" = Exists(Id),
        "case" = Case([Id; 3]), // (case <scrutinee> (<condition> <then> <condition> <then> ...) <else>)

        // a "list" of nodes
        "nodes" = Nodes(Box<[Id]>),

        // relational expression
        "dummy" = DummyScan,
        "empty" = EmptyPlan,
        "project" = Project([Id; 2]),            // (project <source> (<exprs> ...))
        "filter" = Filter([Id; 2]),              // (filter <source> <predicate>)
        Join(ir::JoinKind, [Id; 2]),             // (join <join-kind> <lhs> <rhs>)
        "unnest" = Unnest(Id),                   // (unnest <array-expr>)
        "order" = Order([Id; 2]),                // (order <source> (<order-exprs>...))
        "limit" = Limit([Id; 2]),                // (limit <source> <limit>)
        "scan" = TableScan(Id),                  // (scan <table>)
        "agg" = Aggregate([Id; 3]),              // (agg <source> <group_by>... <aggregates-calls>...)
        "strict-limit" = StrictLimit([Id; 3]),   // (strict_limit <source> <limit> <message>)
        "values" = Values(Box<[Id]>),

        "insert" = Insert([Id; 3]),    // (insert <table> <source> <returning>)
        "update" = Update([Id; 3]),    // (update <table> <source> <returning>)

        // pseudo expressions
        "desc" = Desc(Id),
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
    pub fn build(&mut self, query: &ir::QueryPlan) -> Id {
        self.build_query(query)
    }

    // pub(crate) fn optimize(&mut self, rewrites: &[Rewrite]) {
    //     self.egraph = egg::Runner::<Node, (), ()>::new(())
    //         .with_egraph(mem::take(&mut self.egraph))
    //         .run(rewrites)
    //         .egraph;
    // }

    pub fn finalize(mut self, root: Id) -> Query {
        struct CostFunction;

        impl egg::CostFunction<Node> for CostFunction {
            type Cost = usize;

            fn cost<C>(&mut self, node: &Node, _costs: C) -> Self::Cost
            where
                C: FnMut(Id) -> Self::Cost,
            {
                match node {
                    Node::Subquery(..) | Node::Exists(..) => 100000,
                    _ => 1,
                }
            }
        }

        let extractor = egg::Extractor::new(&self.egraph, CostFunction);
        let (cost, best) = extractor.find_best(root);
        tracing::debug!("best cost: {}", cost);
        self.egraph.rebuild();

        // create a new egraph with only the optimal nodes
        let mut optimized_egraph = egg::EGraph::default();

        let root = optimized_egraph.add_expr(&best);
        optimized_egraph.rebuild();

        Query::new(optimized_egraph, root)
    }

    fn build_query(&mut self, query: &ir::QueryPlan) -> Id {
        let expr = match query {
            ir::QueryPlan::DummyScan => Node::DummyScan,
            ir::QueryPlan::Empty { .. } => Node::EmptyPlan,
            ir::QueryPlan::Aggregate { aggregates, source, group_by, schema: _ } => {
                let source = self.build_query(source);
                let group_by = self.build_exprs(source, group_by);
                let functions = Node::Nodes(
                    aggregates
                        .iter()
                        .map(|(f, args)| {
                            let f = self.add(Node::Function(f.oid()));
                            let args = self.build_exprs(source, args);
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
                let source = self.build_query(source);
                let projection = self.build_exprs(source, projection);
                Node::Project([source, projection])
            }
            ir::QueryPlan::Filter { source, predicate } => {
                let source = self.build_query(source);
                let predicate = self.build_expr(source, predicate);
                Node::Filter([source, predicate])
            }
            ir::QueryPlan::Unnest { schema: _, expr } => {
                let dummy = self.dummy();
                Node::Unnest(self.build_expr(dummy, expr))
            }
            ir::QueryPlan::Values { values, schema: _ } => {
                let dummy = self.dummy();
                Node::Values(values.iter().map(|exprs| self.build_exprs(dummy, exprs)).collect())
            }
            ir::QueryPlan::Join { schema: _, join, lhs, rhs } => {
                let lhs = self.build_query(lhs);
                let rhs = self.build_query(rhs);
                Node::Join(*join, [lhs, rhs])
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
                let order_exprs =
                    order.iter().map(|expr| self.build_order_expr(source, expr)).collect();
                let order = self.add(Node::Nodes(order_exprs));
                Node::Order([source, order])
            }
            ir::QueryPlan::Insert { table, source, returning, schema: _ } => {
                let source = self.build_query(source);
                let returning = self.build_exprs(source, returning);
                let table = self.add(Node::Table(*table));
                Node::Insert([table, source, returning])
            }
            ir::QueryPlan::Update { table, source, returning, schema: _ } => {
                let source = self.build_query(source);
                let returning = self.build_exprs(source, returning);
                let table = self.add(Node::Table(*table));
                Node::Update([table, source, returning])
            }
            ir::QueryPlan::Union { schema: _, lhs, rhs } => {
                let lhs = self.build_query(lhs);
                let rhs = self.build_query(rhs);
                Node::Union([lhs, rhs])
            }
            ir::QueryPlan::CteScan { name, schema: _ } => Node::CteScan(name.clone()),
            ir::QueryPlan::Cte { cte, child } => {
                let cte_plan = self.build_query(&cte.plan);
                let child = self.build_query(child);
                Node::Cte(Name::clone(&cte.name), [cte_plan, child])
            }
            ir::QueryPlan::Distinct { source } => {
                let source = self.build_query(source);
                Node::Distinct(source)
            }
        };

        self.add(expr)
    }

    fn build_order_expr(&mut self, plan: Id, order_expr: &ir::OrderExpr) -> Id {
        let id = self.build_expr(plan, &order_expr.expr);
        if order_expr.asc { id } else { self.add(Node::Desc(id)) }
    }

    fn add_value(&mut self, value: Value) -> Id {
        self.add(Node::Literal(value))
    }

    fn add(&mut self, expr: Node) -> Id {
        self.egraph.add(expr)
    }

    fn dummy(&mut self) -> Id {
        self.add(Node::DummyScan)
    }

    fn build_exprs(&mut self, plan: Id, exprs: &[ir::Expr]) -> Id {
        let id = Node::Nodes(exprs.iter().map(|expr| self.build_expr(plan, expr)).collect());
        self.add(id)
    }

    fn build_expr(&mut self, plan: Id, expr: &ir::Expr) -> Id {
        let node = match &expr.kind {
            ir::ExprKind::Literal(value) => Node::Literal(value.clone()),
            ir::ExprKind::Array(exprs) => {
                Node::Array(exprs.iter().map(|expr| self.build_expr(plan, expr)).collect())
            }
            ir::ExprKind::Alias { alias: _, expr } => return self.build_expr(plan, expr),
            ir::ExprKind::ColumnRef(col) => Node::ColumnRef(col.clone(), plan),
            ir::ExprKind::FunctionCall { function, args } => {
                let f = self.add(Node::Function(function.oid()));
                let args = self.build_exprs(plan, args);
                Node::Call([f, args])
            }
            ir::ExprKind::UnaryOperator { operator, expr } => {
                let f = self.add(Node::Function(operator.mono_function().oid()));
                let expr = self.build_expr(plan, expr);
                Node::Call([f, self.add(Node::Nodes([expr].into()))])
            }
            ir::ExprKind::BinaryOperator { operator, lhs, rhs } => {
                let f = self.add(Node::Function(operator.mono_function().oid()));
                let lhs = self.build_expr(plan, lhs);
                let rhs = self.build_expr(plan, rhs);
                Node::Call([f, self.add(Node::Nodes([lhs, rhs].into()))])
            }
            ir::ExprKind::Case { scrutinee, cases, else_result } => {
                let scrutinee = self.build_expr(plan, scrutinee);
                let cases = cases
                    .iter()
                    .flat_map(|case| {
                        let when = self.build_expr(plan, &case.when);
                        let then = self.build_expr(plan, &case.then);
                        [when, then]
                    })
                    .collect();

                let else_result =
                    self.build_expr(plan, else_result.as_deref().unwrap_or(&ir::Expr::NULL));

                Node::Case([scrutinee, self.add(Node::Nodes(cases)), else_result])
            }
            ir::ExprKind::Subquery(kind, query) => match kind {
                ir::SubqueryKind::Scalar => Node::Subquery(self.build_query(query)),
                ir::SubqueryKind::Exists => Node::Exists(self.build_query(query)),
            },
            ir::ExprKind::Compiled(expr) => Node::CompiledExpr(expr.clone()),
            ir::ExprKind::Quote(expr) => Node::QuotedExpr(self.build_expr(plan, expr)),
        };

        self.add(node)
    }
}
