use core::fmt;

use egg::Id;
use nsql_core::Oid;
use rustc_hash::FxHashMap;

use crate::node::{EGraph, Node};

#[derive(Clone)]
/// An structured view over the raw nodes
// Not sure what a good name for this is?
pub struct Query {
    egraph: EGraph,
    root: Id,
    expr_map: FxHashMap<Id, ir::Expr>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no display yet")
        // write!(f, "{}", self.plan(self.root))
    }
}

impl Query {
    pub(crate) fn new(egraph: EGraph, root: Id, expr_map: FxHashMap<Id, ir::Expr>) -> Self {
        Self { egraph, root, expr_map }
    }

    #[allow(dead_code)]
    pub(crate) fn egraph(&self) -> &EGraph {
        &self.egraph
    }

    pub fn original_expr(&self, id: Id) -> &ir::Expr {
        self.expr_map.get(&id).expect("no original expr for id")
    }

    pub fn root(&self) -> Plan<'_> {
        self.plan(self.root)
    }

    fn node(&self, id: Id) -> &Node {
        &self.egraph[id].nodes[0]
    }

    fn nodes(&self, id: Id) -> &[Id] {
        match self.node(id) {
            Node::Nodes(nodes) => nodes,
            _ => panic!("expected `Nodes` node"),
        }
    }

    fn function(&self, id: Id) -> Oid<ir::Function> {
        match *self.node(id) {
            Node::Function(f) => f,
            _ => panic!("expected `Function` node"),
        }
    }

    fn exprs(&self, id: Id) -> impl ExactSizeIterator<Item = Expr<'_>> {
        match self.node(id) {
            Node::Nodes(exprs) => exprs.iter().map(|&id| self.expr(id)),
            _ => panic!("expected `Nodes` node"),
        }
    }

    fn expr(&self, id: Id) -> Expr<'_> {
        let kind = match *self.node(id) {
            Node::ColumnRef(idx, _) => ExprKind::ColumnRef(idx),
            Node::Literal(_) => ExprKind::Literal(LiteralExpr(id)),
            Node::Array(ref exprs) => ExprKind::Array(ArrayExpr(exprs)),
            Node::Call([f, args]) => {
                ExprKind::Call(CallExpr { function: self.function(f), args: self.nodes(args) })
            }
            Node::Subquery(_) => todo!(),
            Node::Case([scrutinee, cases, else_expr]) => {
                ExprKind::Case(CaseExpr { scrutinee, cases: self.nodes(cases), else_expr })
            }
            ref node @ (Node::DummyScan
            | Node::Nodes(_)
            | Node::Project(_)
            | Node::Filter(_)
            | Node::Join(_)
            | Node::CrossJoin(_)
            | Node::Unnest(_)
            | Node::Order(_)
            | Node::Limit(_)
            | Node::TableScan(_)
            | Node::Aggregate(_)
            | Node::StrictLimit(_)
            | Node::Values(_)
            | Node::Insert(_)
            | Node::Update(_)
            | Node::Desc(_)
            | Node::JoinOn(_, _)
            | Node::Table(_)
            | Node::Function(_)) => panic!("expected `Expr` node, got `{node}`"),
        };

        Expr { id, kind }
    }

    fn plan(&self, id: Id) -> Plan<'_> {
        match *self.node(id) {
            Node::Case(_) => todo!(),
            Node::DummyScan => Plan::DummyScan,
            Node::Project([source, projection]) => {
                Plan::Projection(Projection { projection, source })
            }
            Node::Filter([source, predicate]) => Plan::Filter(Filter { source, predicate }),
            Node::Join([join_expr, lhs, rhs]) => Plan::Join(Join { join_expr, lhs, rhs }),
            Node::CrossJoin([lhs, rhs]) => Plan::CrossJoin(CrossJoin { lhs, rhs }),
            Node::Unnest(expr) => Plan::Unnest(Unnest { expr }),
            Node::Order([source, order_exprs]) => Plan::Order(Order { source, order_exprs }),
            Node::Limit([source, limit]) => Plan::Limit(Limit { source, limit, msg: None }),
            Node::TableScan(table) => Plan::TableScan(TableScan { table }),
            Node::Aggregate([source, group_by, aggregates]) => {
                Plan::Aggregate(Aggregate { source, group_by, aggregates })
            }
            Node::StrictLimit([source, limit, msg]) => {
                Plan::Limit(Limit { source, limit, msg: Some(msg) })
            }
            Node::Values(ref values) => Plan::Values(Values { values }),
            Node::Insert([table, source, returning]) => {
                Plan::Insert(Insert { table, source, returning })
            }
            Node::Update([table, source, returning]) => {
                Plan::Update(Update { table, source, returning })
            }
            Node::Desc(_)
            | Node::Nodes(_)
            | Node::Table(_)
            | Node::Function(_)
            | Node::Call(_)
            | Node::Literal(_)
            | Node::ColumnRef(..)
            | Node::Array(_)
            | Node::Subquery(_)
            | Node::JoinOn(..) => unreachable!("not a plan node"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Plan<'a> {
    Insert(Insert),
    Update(Update),
    Projection(Projection),
    Join(Join),
    CrossJoin(CrossJoin),
    Limit(Limit),
    Aggregate(Aggregate),
    Filter(Filter),
    Values(Values<'a>),
    Order(Order),
    TableScan(TableScan),
    Unnest(Unnest),
    DummyScan,
}

#[derive(Debug, Copy, Clone)]
pub struct Projection {
    projection: Id,
    source: Id,
}

impl Projection {
    pub fn exprs(self, q: &Query) -> impl ExactSizeIterator<Item = Expr<'_>> + '_ {
        q.exprs(self.projection)
    }

    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Insert {
    table: Id,
    source: Id,
    returning: Id,
}

impl Insert {
    #[inline]
    pub fn table(self, q: &Query) -> Oid<ir::Table> {
        match *q.node(self.table) {
            Node::Table(table) => table,
            _ => panic!("expected `Table` node"),
        }
    }

    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn returning(self, q: &Query) -> impl ExactSizeIterator<Item = Expr<'_>> + '_ {
        q.exprs(self.returning)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Update {
    table: Id,
    source: Id,
    returning: Id,
}

impl Update {
    #[inline]
    pub fn table(self, q: &Query) -> Oid<ir::Table> {
        match *q.node(self.table) {
            Node::Table(table) => table,
            _ => panic!("expected `Table` node"),
        }
    }

    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn returning(self, q: &Query) -> impl ExactSizeIterator<Item = Expr<'_>> + '_ {
        q.exprs(self.returning)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Unnest {
    expr: Id,
}

impl Unnest {
    #[inline]
    pub fn expr(self, q: &Query) -> Expr<'_> {
        q.expr(self.expr)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TableScan {
    table: Id,
}

impl TableScan {
    #[inline]
    pub fn table(self, q: &Query) -> Oid<ir::Table> {
        match *q.node(self.table) {
            Node::Table(table) => table,
            _ => panic!("expected `Table` node"),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Limit {
    source: Id,
    limit: Id,
    msg: Option<Id>,
}

impl Limit {
    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn limit(self, q: &Query) -> u64 {
        match q.expr(self.limit).kind {
            ExprKind::Literal(lit) if let &ir::Value::Int64(limit) = lit.value(q) => {
                limit as u64
            },
            _ => panic!("expected `Literal` text node"),
        }
    }

    #[inline]
    pub fn limit_exceeded_message(self, q: &Query) -> Option<String> {
        match q.expr(self.msg?).kind {
            ExprKind::Literal(lit) if let ir::Value::Text(msg) = lit.value(q) => {
                Some(msg.clone())
            },
            _ => panic!("expected `Literal` text node"),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Aggregate {
    source: Id,
    group_by: Id,
    aggregates: Id,
}

impl Aggregate {
    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn group_by(self, q: &Query) -> impl ExactSizeIterator<Item = Expr<'_>> + '_ {
        q.exprs(self.group_by)
    }

    #[inline]
    pub fn aggregates(self, q: &Query) -> impl ExactSizeIterator<Item = CallExpr<'_>> + '_ {
        q.exprs(self.aggregates).map(|expr| match expr.kind {
            ExprKind::Call(call) => call,
            _ => panic!("expected `Call` node"),
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Order {
    source: Id,
    order_exprs: Id,
}

impl Order {
    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn order_exprs(
        self,
        q: &Query,
    ) -> impl ExactSizeIterator<Item = ir::OrderExpr<Expr<'_>>> + '_ {
        q.nodes(self.order_exprs).iter().map(|&node| match q.node(node) {
            Node::Desc(expr) => ir::OrderExpr { expr: q.expr(*expr), asc: false },
            _ => ir::OrderExpr { expr: q.expr(node), asc: true },
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Filter {
    source: Id,
    predicate: Id,
}

impl Filter {
    #[inline]
    pub fn source(self, q: &Query) -> Plan<'_> {
        q.plan(self.source)
    }

    #[inline]
    pub fn predicate(self, q: &Query) -> Expr<'_> {
        q.expr(self.predicate)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Values<'a> {
    values: &'a [Id],
}

impl<'a> Values<'a> {
    #[inline]
    pub fn rows(
        self,
        g: &'a Query,
    ) -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = Expr<'a>> + 'a> + 'a {
        self.values.iter().map(move |&row| g.exprs(row))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Join {
    join_expr: Id,
    lhs: Id,
    rhs: Id,
}

impl Join {
    #[inline]
    pub fn join_expr(self, q: &Query) -> ir::Join<Expr<'_>> {
        match q.node(self.join_expr) {
            Node::JoinOn(kind, expr) => {
                ir::Join::Constrained(*kind, ir::JoinConstraint::On(q.expr(*expr)))
            }
            _ => panic!("expected `JoinOn` node"),
        }
    }

    #[inline]
    pub fn lhs(self, q: &Query) -> Plan<'_> {
        q.plan(self.lhs)
    }

    #[inline]
    pub fn rhs(self, q: &Query) -> Plan<'_> {
        q.plan(self.rhs)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CrossJoin {
    lhs: Id,
    rhs: Id,
}

impl CrossJoin {
    #[inline]
    pub fn lhs(self, q: &Query) -> Plan<'_> {
        q.plan(self.lhs)
    }

    #[inline]
    pub fn rhs(self, q: &Query) -> Plan<'_> {
        q.plan(self.rhs)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Expr<'a> {
    pub id: Id,
    pub kind: ExprKind<'a>,
}

#[derive(Debug, Clone, Copy)]
pub enum ExprKind<'a> {
    ColumnRef(ir::TupleIndex),
    Literal(LiteralExpr),
    Array(ArrayExpr<'a>),
    Call(CallExpr<'a>),
    Case(CaseExpr<'a>),
}

#[derive(Debug, Clone, Copy)]
pub struct LiteralExpr(Id);

impl LiteralExpr {
    #[inline]
    pub fn value(self, q: &Query) -> &ir::Value {
        match *q.node(self.0) {
            Node::Literal(ref value) => value,
            _ => panic!("expected `Literal` node"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArrayExpr<'a>(&'a [Id]);

impl<'a> ArrayExpr<'a> {
    #[inline]
    pub fn exprs(self, g: &'a Query) -> impl ExactSizeIterator<Item = Expr<'a>> + 'a {
        self.0.iter().map(|&id| g.expr(id))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CallExpr<'a> {
    function: Oid<ir::Function>,
    args: &'a [Id],
}

impl<'a> CallExpr<'a> {
    #[inline]
    pub fn function(self) -> Oid<ir::Function> {
        self.function
    }

    #[inline]
    pub fn args(self, g: &'a Query) -> impl ExactSizeIterator<Item = Expr<'a>> + 'a {
        self.args.iter().map(|&id| g.expr(id))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CaseExpr<'a> {
    scrutinee: Id,
    cases: &'a [Id],
    else_expr: Id,
}

impl<'a> CaseExpr<'a> {
    #[inline]
    pub fn scrutinee(self, g: &'a Query) -> Expr<'a> {
        g.expr(self.scrutinee)
    }

    #[inline]
    pub fn cases(self, g: &'a Query) -> impl ExactSizeIterator<Item = (Expr<'a>, Expr<'a>)> + 'a {
        self.cases
            .iter()
            .array_chunks::<2>()
            .map(move |[when, then]| (g.expr(*when), g.expr(*then)))
    }

    #[inline]
    pub fn else_expr(self, g: &'a Query) -> Expr<'a> {
        g.expr(self.else_expr)
    }
}
