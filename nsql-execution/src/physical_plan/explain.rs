use std::fmt;

use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage::Transaction;
use nsql_storage_engine::StorageEngine;

use crate::pipeline::MetaPipeline;
use crate::{PhysicalNode, RootPipeline};

pub type Result<T = ()> = std::result::Result<T, fmt::Error>;

pub trait Explain<S: StorageEngine> {
    fn explain(
        &self,
        catalog: &Catalog<S>,
        tx: &S::Transaction<'_>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result;
}

pub(crate) fn explain_pipeline<'a, 'env, S: StorageEngine>(
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    root_pipeline: &'a RootPipeline<S>,
) -> RootPipelineExplainer<'a, 'env, S> {
    RootPipelineExplainer { catalog, tx, root_pipeline }
}

pub struct RootPipelineExplainer<'a, 'env, S: StorageEngine> {
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    root_pipeline: &'a RootPipeline<S>,
}

pub struct MetaPipelineExplainer<'a, 'env, S: StorageEngine> {
    root: &'a RootPipelineExplainer<'a, 'env, S>,
    tx: &'a S::Transaction<'env>,
    indent: usize,
}

impl<'a, 'env, S: StorageEngine> MetaPipelineExplainer<'a, 'env, S> {
    fn explain_meta_pipeline(
        &self,
        f: &mut fmt::Formatter<'_>,
        meta_pipeline: Idx<MetaPipeline<S>>,
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

impl<S: StorageEngine> fmt::Display for RootPipelineExplainer<'_, '_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        MetaPipelineExplainer { root: self, tx: self.tx, indent: 0 }
            .explain_meta_pipeline(f, self.root_pipeline.arena.root())
    }
}

pub(crate) fn explain<'a, 'env, S: StorageEngine>(
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    node: &'a dyn PhysicalNode<S>,
) -> PhysicalNodeExplainer<'a, 'env, S> {
    PhysicalNodeExplainer { catalog, tx, node, indent: 0 }
}

pub struct PhysicalNodeExplainer<'a, 'env, S: StorageEngine> {
    catalog: &'a Catalog<S>,
    tx: &'a S::Transaction<'env>,
    node: &'a dyn PhysicalNode<S>,
    indent: usize,
}

impl<'a, 'env, S: StorageEngine> PhysicalNodeExplainer<'a, 'env, S> {
    fn explain_child(&self, node: &'a dyn PhysicalNode<S>) -> PhysicalNodeExplainer<'a, 'env, S> {
        PhysicalNodeExplainer { catalog: self.catalog, tx: self.tx, node, indent: self.indent + 2 }
    }
}

impl<'a, 'env, S: StorageEngine> fmt::Display for PhysicalNodeExplainer<'a, 'env, S> {
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
