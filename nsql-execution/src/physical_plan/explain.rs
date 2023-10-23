use std::fmt;

use nsql_arena::{ArenaMap, Idx};
use nsql_catalog::{Catalog, TransactionContext};
use nsql_storage_engine::StorageEngine;

use super::PhysicalPlan;
use crate::pipeline::MetaPipeline;
use crate::{ExecutionMode, PhysicalNodeArena, PhysicalNodeId, RootPipeline};

pub type Result<T = ()> = anyhow::Result<T>;

pub trait Explain<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M>;

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result;

    fn display<'a>(
        &'a self,
        catalog: Catalog<'env, S>,
        tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    ) -> Display<'a, 'env, 'txn, S, M> {
        Display { catalog, tx, explain: self.as_dyn() }
    }
}

pub struct Display<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    catalog: Catalog<'env, S>,
    tx: &'a dyn TransactionContext<'env, 'txn, S, M>,
    explain: &'a dyn Explain<'env, 'txn, S, M>,
}

impl<'a, 'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> fmt::Display
    for Display<'a, 'env, 'txn, S, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.explain.explain(self.catalog, self.tx, f).map_err(|err| {
            tracing::error!("failed to explain: {err}");
            fmt::Error
        })
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for RootPipeline<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result {
        RootPipelineExplainer { root_pipeline: self }.explain(catalog, tx, f)
    }
}

struct RootPipelineExplainer<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    root_pipeline: &'a RootPipeline<'env, 'txn, S, M>,
}

pub struct MetaPipelineExplainer<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    root: &'a RootPipelineExplainer<'a, 'env, 'txn, S, M>,
    indent: usize,
}

impl<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    MetaPipelineExplainer<'a, 'env, 'txn, S, M>
{
    fn explain_meta_pipeline(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
        meta_pipeline: Idx<MetaPipeline<'env, 'txn, S, M>>,
    ) -> Result {
        let arena = &self.root.root_pipeline.arena;
        writeln!(
            f,
            "{:indent$}metapipeline #{}",
            "",
            meta_pipeline.into_raw(),
            indent = self.indent
        )?;
        let meta_pipeline = &arena[meta_pipeline];

        for &pipeline in &meta_pipeline.pipelines {
            writeln!(
                f,
                "{:indent$}pipeline #{}",
                "",
                pipeline.into_raw(),
                indent = self.indent + 2
            )?;

            let pipeline = &arena[pipeline];
            let nodes = &self.root.root_pipeline.nodes;
            for node in pipeline.nodes().rev() {
                write!(f, "{:indent$} node #{} ", "", node.into_raw(), indent = self.indent + 4)?;
                nodes[node].explain(catalog, tx, f)?;
                writeln!(f)?;
            }
        }

        for &child in &meta_pipeline.children {
            writeln!(f)?;
            MetaPipelineExplainer { root: self.root, indent: self.indent + 6 }
                .explain_meta_pipeline(catalog, tx, f, child)?;
        }
        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for RootPipelineExplainer<'_, 'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result {
        MetaPipelineExplainer { root: self, indent: 0 }.explain_meta_pipeline(
            catalog,
            tx,
            f,
            self.root_pipeline.arena.root(),
        )
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalPlan<'env, 'txn, S, M> {
    pub fn explain_tree(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
    ) -> ExplainTree {
        let mut nodes = ArenaMap::default();
        let root = self.explain_node(&mut nodes, catalog, tx, self.root, 0);
        ExplainTree { root, nodes }
    }

    fn explain_node(
        &self,
        nodes: &mut ArenaMap<PhysicalNodeId, ExplainNode>,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        id: PhysicalNodeId,
        depth: usize,
    ) -> PhysicalNodeId {
        let node = &self.nodes[id];
        let children = node
            .children()
            .iter()
            .map(|&child| self.explain_node(nodes, catalog, tx, child, 1 + depth))
            .collect();

        let prev = nodes.insert(
            id,
            ExplainNode {
                depth,
                children,
                fmt: node.display(catalog, tx).to_string(),
                annotations: Default::default(),
            },
        );
        assert!(prev.is_none());
        id
    }
}

/// A materialized tree of `ExplainNode` which can be formatted as an explain (analyze) plan.
/// This is useful as it contains no references and can be augmented with additional information post construction.
pub struct ExplainTree {
    root: PhysicalNodeId,
    nodes: ArenaMap<PhysicalNodeId, ExplainNode>,
}

impl ExplainTree {
    pub fn annotate(&mut self, fmt: ArenaMap<PhysicalNodeId, (String, String)>) {
        for (id, kv) in fmt {
            if let Some(node) = self.nodes.get_mut(id) {
                node.annotations.push(kv);
            }
        }
    }

    fn fmt_node(&self, id: PhysicalNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let node = &self.nodes[id];
        write!(f, "{:indent$}{}", "", node.fmt, indent = node.depth * 2)?;
        for (k, v) in &node.annotations {
            write!(f, " {}={}", k, v)?;
        }

        writeln!(f)?;

        for &child in &node.children[..] {
            self.fmt_node(child, f)?;
        }
        Ok(())
    }
}

impl fmt::Display for ExplainTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_node(self.root, f)
    }
}

struct ExplainNode {
    fmt: String,
    depth: usize,
    annotations: Vec<(String, String)>,
    children: Box<[PhysicalNodeId]>,
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalPlan<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result {
        PhysicalNodeExplainer { nodes: &self.nodes, node: self.root(), indent: 0 }
            .explain(catalog, tx, f)
    }
}

pub struct PhysicalNodeExplainer<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    nodes: &'a PhysicalNodeArena<'env, 'txn, S, M>,
    node: PhysicalNodeId,
    indent: usize,
}

impl<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNodeExplainer<'a, 'env, 'txn, S, M>
{
    fn explain_child(&self, node: PhysicalNodeId) -> PhysicalNodeExplainer<'a, 'env, 'txn, S, M> {
        PhysicalNodeExplainer { nodes: self.nodes, node, indent: self.indent + 2 }
    }
}

impl<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, 'txn, S, M>
    for PhysicalNodeExplainer<'a, 'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        self.nodes[self.node].explain(catalog, tx, f)?;
        writeln!(f)?;

        for &child in self.nodes[self.node].children() {
            self.explain_child(child).explain(catalog, tx, f)?;
        }

        Ok(())
    }
}
