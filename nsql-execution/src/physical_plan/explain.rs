use std::fmt;

use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::Transaction;
use nsql_storage_engine::StorageEngine;

use crate::pipeline::MetaPipeline;
use crate::{PhysicalNode, RootPipeline};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain<S> {
    fn explain(&self, catalog: &Catalog<S>, tx: &Transaction, f: &mut fmt::Formatter<'_>)
    -> Result;
}

pub(crate) fn explain_pipeline<'a, S>(
    catalog: &'a Catalog<S>,
    tx: &'a Transaction,
    root_pipeline: &'a RootPipeline,
) -> impl fmt::Display + 'a {
    RootPipelineExplainer { catalog, tx, root_pipeline }
}

pub struct RootPipelineExplainer<'a, S> {
    catalog: &'a Catalog<S>,
    tx: &'a Transaction,
    root_pipeline: &'a RootPipeline,
}

impl<S: StorageEngine> RootPipelineExplainer<'_, S> {}

pub struct MetaPipelineExplainer<'a, S> {
    root: &'a RootPipelineExplainer<'a, S>,
    indent: usize,
}

impl<'a, S> MetaPipelineExplainer<'a, S> {
    fn explain_meta_pipeline(
        &self,
        f: &mut fmt::Formatter<'_>,
        meta_pipeline: Idx<MetaPipeline>,
    ) -> fmt::Result {
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
            for node in pipeline.nodes().rev() {
                write!(f, "{:indent$}", "", indent = self.indent + 4)?;
                node.explain(self.root.catalog, self.root.tx, f)?;
                writeln!(f)?;
            }
        }

        for &child in &meta_pipeline.children {
            writeln!(f)?;
            MetaPipelineExplainer { root: self.root, indent: self.indent + 6 }
                .explain_meta_pipeline(f, child)?;
        }
        Ok(())
    }
}

impl<S> fmt::Display for RootPipelineExplainer<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MetaPipelineExplainer { root: self, indent: 0 }
            .explain_meta_pipeline(f, self.root_pipeline.arena.root())
    }
}

pub(crate) fn explain<'a, S>(
    catalog: &'a Catalog<S>,
    tx: &'a Transaction,
    node: &'a dyn PhysicalNode<S>,
) -> impl fmt::Display + 'a {
    PhysicalNodeExplainer { catalog, tx, node, indent: 0 }
}

pub struct PhysicalNodeExplainer<'a, S> {
    catalog: &'a Catalog<S>,
    tx: &'a Transaction,
    node: &'a dyn PhysicalNode<S>,
    indent: usize,
}

impl<'a, S> PhysicalNodeExplainer<'a, S> {
    fn explain_child(&self, node: &'a dyn PhysicalNode<S>) -> PhysicalNodeExplainer<'_, S> {
        PhysicalNodeExplainer { catalog: self.catalog, tx: self.tx, node, indent: self.indent + 2 }
    }
}

impl<S> fmt::Display for PhysicalNodeExplainer<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        self.node.explain(self.catalog, self.tx, f)?;
        writeln!(f)?;

        for child in self.node.children() {
            self.explain_child(child.as_ref()).fmt(f)?;
        }

        Ok(())
    }
}
