use dot_structures as dot;
use nsql_arena::Idx;

use crate::pipeline::{MetaPipeline, Pipeline, PipelineArena};
use crate::RootPipeline;

impl RootPipeline {
    pub fn dot(&self) -> String {
        self.graph().print(&mut PrinterContext::default())
    }

    pub fn graph(&self) -> dot::Graph {
        let subgraph = self.arena[self.root].graph(self.root, &self.arena);
        dot::Graph::DiGraph { id: subgraph.id, strict: true, stmts: subgraph.stmts }
    }
}

impl MetaPipeline {
    fn graph(&self, idx: Idx<Self>, arena: &PipelineArena) -> dot::Subgraph {
        dbg!(idx);
        let node_id = format!("metapipeline{}", idx.into_raw());
        let mut stmts = vec![];
        for &child in &self.children {
            let subgraph = arena[child].graph(child, arena);
            stmts.push(dot::Stmt::Edge(dot::Edge {
                ty: dot::EdgeTy::Pair(
                    dot::Vertex::S(subgraph),
                    dot::Vertex::N(dot::NodeId(dot::Id::Plain(node_id.to_string()), None)),
                ),
                attributes: vec![],
            }));
        }

        for &pipeline in &self.pipelines {
            println!("pipeline: {:?} -> parent {:?}", pipeline, idx);
            let subgraph = arena[pipeline].graph(pipeline);
            stmts.push(dot::Stmt::Edge(dot::Edge {
                ty: dot::EdgeTy::Pair(
                    dot::Vertex::S(subgraph),
                    dot::Vertex::N(dot::NodeId(dot::Id::Plain(node_id.to_string()), None)),
                ),
                attributes: vec![],
            }));
        }

        dot::Subgraph { id: dot::Id::Plain(node_id), stmts }
    }
}

impl Pipeline {
    fn graph(&self, idx: Idx<Self>) -> dot::Subgraph {
        let node_id = format!("pipeline{}", idx.into_raw());
        let mut nodes = vec![];

        nodes.push(dot::Stmt::Node(dot::Node::new(
            dot::NodeId(dot::Id::Plain(self.source.desc().into()), None),
            vec![],
        )));

        for op in &self.operators {
            nodes.push(dot::Stmt::Node(dot::Node::new(
                dot::NodeId(dot::Id::Plain(op.desc().into()), None),
                vec![],
            )));
        }

        nodes.push(dot::Stmt::Node(dot::Node::new(
            dot::NodeId(dot::Id::Plain(self.sink.desc().into()), None),
            vec![],
        )));

        let mut stmts = vec![];
        let mut prev_node_id = None;
        for node in nodes {
            let node_id = match &node {
                dot::Stmt::Node(node) => node.id.clone(),
                _ => unreachable!(),
            };

            if let Some(prev_node_id) = prev_node_id {
                stmts.push(dot::Stmt::Edge(dot::Edge {
                    ty: dot::EdgeTy::Pair(
                        dot::Vertex::N(prev_node_id),
                        dot::Vertex::N(node_id.clone()),
                    ),
                    attributes: vec![],
                }));
            }

            prev_node_id = Some(node_id);

            stmts.push(node);
        }

        dot::Subgraph { id: dot::Id::Plain(node_id), stmts }
    }
}

use dot_structures::{
    Attribute, Edge, EdgeTy, Graph, GraphAttributes, Id, Node, NodeId, Port, Stmt, Subgraph, Vertex,
};

/// Context allows to customize the output of the file.
/// # Example:
/// ```rust
/// fn ctx() {
///     use self::graphviz_rust::printer::PrinterContext;
///
///     let mut ctx = PrinterContext::default();
///     ctx.always_inline();
///     ctx.with_indent_step(4);
/// }
/// ```
pub struct PrinterContext {
    /// internal flag which is decoupled from the graph
    is_digraph: bool,
    /// a flag adds a semicolon at the end of the line
    semi: bool,
    /// an initial indent. 0 by default
    indent: usize,
    /// a step of the indent. 2 by default
    indent_step: usize,
    /// a line separator. can be empty
    l_s: String,
    /// a len of the text to keep on one line
    inline_size: usize,
    l_s_i: String,
    l_s_m: String,
}

impl PrinterContext {
    fn indent(&self) -> String {
        if self.is_inline_on() { "".to_string() } else { " ".repeat(self.indent) }
    }
    fn indent_grow(&mut self) {
        if !self.is_inline_on() {
            self.indent += self.indent_step
        }
    }
    fn indent_shrink(&mut self) {
        if !self.is_inline_on() {
            self.indent -= self.indent_step
        }
    }

    fn is_inline_on(&self) -> bool {
        self.l_s == self.l_s_i
    }

    fn inline_mode(&mut self) {
        self.l_s = self.l_s_i.clone()
    }

    fn multiline_mode(&mut self) {
        self.l_s = self.l_s_m.clone()
    }
}

impl Default for PrinterContext {
    fn default() -> Self {
        PrinterContext {
            is_digraph: false,
            semi: false,
            indent: 0,
            indent_step: 2,
            l_s: "\n".to_string(),
            inline_size: 90,
            l_s_i: "".to_string(),
            l_s_m: "\n".to_string(),
        }
    }
}

// below is copied from graphviz-rs as I don't want the rest of the dependencies
pub trait DotPrinter {
    fn print(&self, ctx: &mut PrinterContext) -> String;
}

impl DotPrinter for Id {
    fn print(&self, _ctx: &mut PrinterContext) -> String {
        match self {
            Id::Html(v) | Id::Escaped(v) | Id::Plain(v) => v.clone(),
            Id::Anonymous(_) => "".to_string(),
        }
    }
}

impl DotPrinter for Port {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        match self {
            Port(Some(id), Some(d)) => format!(":{}:{}", id.print(ctx), d),
            Port(None, Some(d)) => format!(":{}", d),
            Port(Some(id), None) => format!(":{}", id.print(ctx)),
            _ => unreachable!(""),
        }
    }
}

impl DotPrinter for NodeId {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        match self {
            NodeId(id, None) => id.print(ctx),
            NodeId(id, Some(port)) => [id.print(ctx), port.print(ctx)].join(""),
        }
    }
}

impl DotPrinter for Attribute {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        match self {
            Attribute(l, r) => format!("{}={}", l.print(ctx), r.print(ctx)),
        }
    }
}

impl DotPrinter for Vec<Attribute> {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        let attrs: Vec<String> = self.iter().map(|e| e.print(ctx)).collect();
        if attrs.is_empty() { "".to_string() } else { format!("[{}]", attrs.join(",")) }
    }
}

impl DotPrinter for GraphAttributes {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        match self {
            GraphAttributes::Graph(attrs) => format!("graph{}", attrs.print(ctx)),
            GraphAttributes::Node(attrs) => format!("node{}", attrs.print(ctx)),
            GraphAttributes::Edge(attrs) => format!("edge{}", attrs.print(ctx)),
        }
    }
}

impl DotPrinter for Node {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        format!("{}{}", self.id.print(ctx), self.attributes.print(ctx))
    }
}

impl DotPrinter for Vertex {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        match self {
            Vertex::N(el) => el.print(ctx),
            Vertex::S(el) => el.print(ctx),
        }
    }
}

impl DotPrinter for Subgraph {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        let indent = ctx.indent();
        ctx.indent_grow();
        let header = format!("subgraph {} {{{}", self.id.print(ctx), ctx.l_s);
        let r = format!("{}{}{}{}}}", header, self.stmts.print(ctx), ctx.l_s, indent);
        ctx.indent_shrink();
        r
    }
}

impl DotPrinter for Graph {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        ctx.indent_grow();

        match self {
            Graph::Graph { id, strict, stmts } if *strict => {
                ctx.is_digraph = false;
                let body = stmts.print(ctx);
                format!("strict graph {} {{{}{}{}}}", id.print(ctx), ctx.l_s, body, ctx.l_s)
            }
            Graph::Graph { id, strict: _, stmts } => {
                ctx.is_digraph = false;
                let body = stmts.print(ctx);
                format!("graph {} {{{}{}{}}}", id.print(ctx), ctx.l_s, body, ctx.l_s)
            }
            Graph::DiGraph { id, strict, stmts } if *strict => {
                ctx.is_digraph = true;
                let body = stmts.print(ctx);
                format!("strict digraph {} {{{}{}{}}}", id.print(ctx), ctx.l_s, body, ctx.l_s)
            }
            Graph::DiGraph { id, strict: _, stmts } => {
                ctx.is_digraph = true;
                let body = stmts.print(ctx);
                format!("digraph {} {{{}{}{}}}", id.print(ctx), ctx.l_s, body, ctx.l_s)
            }
        }
    }
}

impl DotPrinter for Vec<Stmt> {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        ctx.indent_grow();
        let attrs: Vec<String> = self.iter().map(|e| e.print(ctx)).collect();
        ctx.indent_shrink();
        attrs.join(ctx.l_s.as_str())
    }
}

impl DotPrinter for Stmt {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        let end = if ctx.semi { ";" } else { "" };
        let indent = ctx.indent();
        match self {
            Stmt::Node(e) => format!("{}{}{}", indent, e.print(ctx), end),
            Stmt::Subgraph(e) => format!("{}{}{}", indent, e.print(ctx), end),
            Stmt::Attribute(e) => format!("{}{}{}", indent, e.print(ctx), end),
            Stmt::GAttribute(e) => format!("{}{}{}", indent, e.print(ctx), end),
            Stmt::Edge(e) => format!("{}{}{}", indent, e.print(ctx), end),
        }
    }
}

fn print_edge(edge: &Edge, ctx: &mut PrinterContext) -> String {
    let bond = if ctx.is_digraph { "->" } else { "--" };
    match edge {
        Edge { ty: EdgeTy::Pair(l, r), attributes } => {
            format!("{} {} {} {}", l.print(ctx), bond, r.print(ctx), attributes.print(ctx))
        }
        Edge { ty: EdgeTy::Chain(vs), attributes } => {
            let mut iter = vs.iter();
            let h = iter.next().unwrap().print(ctx);
            let mut chain = h;
            for el in iter {
                chain = format!("{} {} {}", chain, bond, el.print(ctx))
            }
            format!("{}{}", chain, attributes.print(ctx))
        }
    }
}

impl DotPrinter for Edge {
    fn print(&self, ctx: &mut PrinterContext) -> String {
        let mut edge_str = print_edge(self, ctx);
        if edge_str.len() <= ctx.inline_size && !ctx.is_inline_on() {
            ctx.inline_mode();
            edge_str = print_edge(self, ctx);
            ctx.multiline_mode();
        }

        edge_str
    }
}
