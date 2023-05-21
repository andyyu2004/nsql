use std::fmt;

use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage_engine::StorageEngine;

use crate::pipeline::MetaPipeline;
use crate::{ExecutionMode, PhysicalNode, RootPipeline};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain<S: StorageEngine> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result;
}

pub(crate) fn explain_pipeline<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    root_pipeline: &'a RootPipeline<'env, S, M>,
) -> RootPipelineExplainer<'a, 'env, S, M> {
    RootPipelineExplainer { catalog, tx, root_pipeline }
}

pub struct RootPipelineExplainer<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    root_pipeline: &'a RootPipeline<'env, S, M>,
}

pub struct MetaPipelineExplainer<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    root: &'a RootPipelineExplainer<'a, 'env, S, M>,
    tx: &'a S::Transaction<'env>,
    indent: usize,
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> MetaPipelineExplainer<'a, 'env, S, M> {
    fn explain_meta_pipeline(
        &self,
        f: &mut fmt::Formatter<'_>,
        meta_pipeline: Idx<MetaPipeline<'env, S, M>>,
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
                node.explain(self.root.catalog, self.tx, f)?;
                writeln!(f)?;
            }
        }

        for &child in &meta_pipeline.children {
            writeln!(f)?;
            MetaPipelineExplainer { root: self.root, tx: self.tx, indent: self.indent + 6 }
                .explain_meta_pipeline(f, child)?;
        }
        Ok(())
    }
}

impl<'env, S: StorageEngine, M: ExecutionMode<'env, S>> fmt::Display
    for RootPipelineExplainer<'_, 'env, S, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MetaPipelineExplainer { root: self, tx: self.tx, indent: 0 }
            .explain_meta_pipeline(f, self.root_pipeline.arena.root())
    }
}

pub(crate) fn explain<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>>(
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    node: &'a dyn PhysicalNode<'env, S, M>,
) -> PhysicalNodeExplainer<'a, 'env, S, M> {
    PhysicalNodeExplainer { catalog, tx, node, indent: 0 }
}

pub struct PhysicalNodeExplainer<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> {
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    node: &'a dyn PhysicalNode<'env, S, M>,
    indent: usize,
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNodeExplainer<'a, 'env, S, M> {
    fn explain_child(
        &self,
        node: &'a dyn PhysicalNode<'env, S, M>,
    ) -> PhysicalNodeExplainer<'a, 'env, S, M> {
        PhysicalNodeExplainer { catalog: self.catalog, tx: self.tx, node, indent: self.indent + 2 }
    }
}

impl<'a, 'env, S: StorageEngine, M: ExecutionMode<'env, S>> fmt::Display
    for PhysicalNodeExplainer<'a, 'env, S, M>
{
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
