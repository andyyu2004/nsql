use std::fmt;

use egg::Id;
use itertools::Itertools;
use nsql_core::{Name, Oid};
use nsql_storage::eval;

use crate::node::{EGraph, Node};

/// An structured view over the raw nodes
// Not sure what a good name for this is?
pub struct Query {
    egraph: EGraph,
    root: Id,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no display yet")
        // write!(f, "{}", self.plan(self.root))
    }
}

impl Query {
    pub(crate) fn new(egraph: EGraph, root: Id) -> Self {
        assert!(egraph.clean);
        for eclass in egraph.classes() {
            assert_eq!(eclass.nodes.len(), 1, "expected eclass to have exactly one unique node");
        }

        Self { egraph, root }
    }

    #[allow(dead_code)]
    pub(crate) fn egraph(&self) -> &EGraph {
        &self.egraph
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
        match *self.node(id) {
            Node::ColumnRef(ref col, _) => Expr::ColumnRef(col.clone()),
            Node::Literal(_) => Expr::Literal(LiteralExpr(id)),
            Node::Array(ref exprs) => Expr::Array(ArrayExpr(exprs)),
            Node::CompiledExpr(ref expr) => Expr::Compiled(expr),
            Node::Call([f, args]) => {
                Expr::Call(CallExpr { function: self.function(f), args: self.nodes(args) })
            }
            Node::Case([scrutinee, cases, else_expr]) => {
                Expr::Case(CaseExpr { scrutinee, cases: self.nodes(cases), else_expr })
            }
            Node::QuotedExpr(expr) => Expr::Quote(QuotedExpr(expr)),
            Node::Subquery(_) | Node::Exists(..) => {
                panic!("subquery nodes should have been flattened during optimization")
            }
            ref node @ (Node::DummyScan
            | Node::Nodes(_)
            | Node::Project(_)
            | Node::Filter(_)
            | Node::Join(..)
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
            | Node::Table(_)
            | Node::Function(_)
            | Node::CteScan(..)
            | Node::Cte(..)
            | Node::Union(..)) => panic!("expected `Expr` node, got `{node}`"),
        }
    }

    fn plan(&self, id: Id) -> Plan<'_> {
        match *self.node(id) {
            Node::DummyScan => Plan::DummyScan,
            Node::Project([source, projection]) => {
                Plan::Projection(Projection { projection, source })
            }
            Node::Filter([source, predicate]) => Plan::Filter(Filter { source, predicate }),
            Node::Join(join_kind, [lhs, rhs]) => Plan::Join(Join { kind: join_kind, lhs, rhs }),
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

            Node::Union([lhs, rhs]) => Plan::Union(Union { lhs, rhs }),
            Node::Cte(ref name, [cte_plan, child]) => Plan::Cte(Cte { name, cte_plan, child }),
            Node::CteScan(ref name) => Plan::CteScan(CteScan { name }),
            Node::Desc(_)
            | Node::QuotedExpr(_)
            | Node::Nodes(_)
            | Node::Table(_)
            | Node::Function(_)
            | Node::Call(_)
            | Node::Literal(_)
            | Node::ColumnRef(..)
            | Node::Array(_)
            | Node::Exists(_)
            | Node::Case(_)
            | Node::CompiledExpr(..)
            | Node::Subquery(_) => unreachable!("not a plan node"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Plan<'a> {
    Insert(Insert),
    Update(Update),
    Projection(Projection),
    Join(Join),
    Limit(Limit),
    Aggregate(Aggregate),
    Filter(Filter),
    Values(Values<'a>),
    Order(Order),
    Union(Union),
    TableScan(TableScan),
    Unnest(Unnest),
    CteScan(CteScan<'a>),
    Cte(Cte<'a>),
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
        match q.expr(self.limit){
            Expr::Literal(lit) if let &ir::Value::Int64(limit) = lit.value(q) => {
                limit as u64
            },
            _ => panic!("expected `Literal` text node"),
        }
    }

    #[inline]
    pub fn limit_exceeded_message(self, q: &Query) -> Option<String> {
        match q.expr(self.msg?){
            Expr::Literal(lit) if let ir::Value::Text(msg) = lit.value(q) => {
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
        q.exprs(self.aggregates).map(|expr| match expr {
            Expr::Call(call) => call,
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
pub struct Cte<'a> {
    name: &'a Name,
    cte_plan: Id,
    child: Id,
}

impl Cte<'_> {
    #[inline]
    pub fn name(self) -> Name {
        Name::clone(self.name)
    }

    #[inline]
    pub fn cte_plan(self, q: &Query) -> Plan<'_> {
        q.plan(self.cte_plan)
    }

    #[inline]
    pub fn child(self, q: &Query) -> Plan<'_> {
        q.plan(self.child)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CteScan<'a> {
    name: &'a Name,
}

impl CteScan<'_> {
    #[inline]
    pub fn name(self) -> Name {
        Name::clone(self.name)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Union {
    lhs: Id,
    rhs: Id,
}

impl Union {
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
pub struct Join {
    kind: ir::JoinKind,
    lhs: Id,
    rhs: Id,
}

impl Join {
    #[inline]
    pub fn kind(self) -> ir::JoinKind {
        self.kind
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

#[derive(Debug, Clone)]
pub enum Expr<'a> {
    ColumnRef(ir::ColumnRef),
    Literal(LiteralExpr),
    Array(ArrayExpr<'a>),
    Call(CallExpr<'a>),
    Case(CaseExpr<'a>),
    Quote(QuotedExpr),
    Compiled(&'a eval::Expr),
}

impl<'q> Expr<'q> {
    pub fn display(self, q: &'q Query) -> impl fmt::Display + 'q {
        ExprDisplay { q, expr: self }
    }
}

struct ExprDisplay<'q> {
    q: &'q Query,
    expr: Expr<'q>,
}

impl<'q> fmt::Display for ExprDisplay<'q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let q = self.q;
        match &self.expr {
            Expr::ColumnRef(col) => col.fmt(f),
            Expr::Literal(lit) => write!(f, "{}", lit.value(q)),
            Expr::Array(array) => {
                write!(f, "[{}]", array.exprs(q).map(|expr| expr.display(q)).format(", "))
            }
            Expr::Call(call) => {
                write!(
                    f,
                    "{}({})",
                    call.function(),
                    call.args(q).map(|expr| expr.display(q)).format(", ")
                )
            }
            Expr::Case(case) => {
                write!(f, "CASE {} ", case.scrutinee(q).display(q))?;
                for (when, then) in case.cases(q) {
                    write!(f, "WHEN {} THEN {}", when.display(q), then.display(q))?;
                }

                write!(f, " ELSE {} ", case.else_expr(q).display(q))?;
                write!(f, "END")
            }
            Expr::Compiled(expr) => write!(f, "{expr}"),
            Expr::Quote(expr) => write!(f, "'({})", expr.expr(q).display(q)),
        }
    }
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
pub struct QuotedExpr(Id);

impl QuotedExpr {
    #[inline]
    pub fn expr(self, q: &Query) -> Expr<'_> {
        q.expr(self.0)
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
