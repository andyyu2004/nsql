use std::fmt;
use std::sync::Arc;

use nsql_arena::Idx;
use nsql_catalog::Catalog;
use nsql_storage_engine::{StorageEngine, Transaction};

use crate::pipeline::MetaPipeline;
use crate::{ExecutionMode, PhysicalNode, RootPipeline};

pub type Result<T = ()> = anyhow::Result<T>;

pub trait Explain<'env, S: StorageEngine> {
    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result;
}

pub(crate) fn display<'a, 'env: 'a, S: StorageEngine>(
    catalog: Catalog<'env, S>,
    tx: &'a dyn Transaction<'env, S>,
    explain: &'a dyn Explain<'env, S>,
) -> Display<'a, 'env, S> {
    Display { catalog, tx, explain, marker: std::marker::PhantomData }
}

pub(crate) struct Display<'a, 'env, S: StorageEngine> {
    catalog: Catalog<'env, S>,
    tx: &'a dyn Transaction<'env, S>,
    explain: &'a dyn Explain<'env, S>,
    marker: std::marker::PhantomData<&'env ()>,
}

impl<'a, 'env, S: StorageEngine> fmt::Display for Display<'a, 'env, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.explain.explain(self.catalog, self.tx, f).map_err(|e| fmt::Error)
    }
}

pub(crate) fn explain_pipeline<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    root_pipeline: &'a RootPipeline<'env, 'txn, S, M>,
) -> RootPipelineExplainer<'a, 'env, 'txn, S, M> {
    RootPipelineExplainer { root_pipeline }
}

pub struct RootPipelineExplainer<'a, 'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
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
        tx: &dyn Transaction<'env, S>,
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
            for node in pipeline.nodes().rev() {
                write!(f, "{:indent$}", "", indent = self.indent + 4)?;
                node.explain(catalog, tx, f)?;
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

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for RootPipelineExplainer<'_, 'env, 'txn, S, M>
{
    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
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

pub(crate) fn explain<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>(
    node: Arc<dyn PhysicalNode<'env, 'txn, S, M> + 'txn>,
) -> PhysicalNodeExplainer<'env, 'txn, S, M> {
    PhysicalNodeExplainer { node, indent: 0 }
}

pub struct PhysicalNodeExplainer<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    indent: usize,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalNodeExplainer<'env, 'txn, S, M>
{
    fn explain_child(
        &self,
        node: Arc<dyn PhysicalNode<'env, 'txn, S, M>>,
    ) -> PhysicalNodeExplainer<'env, 'txn, S, M> {
        PhysicalNodeExplainer { node, indent: self.indent + 2 }
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalNodeExplainer<'env, 'txn, S, M>
{
    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> Result {
        write!(f, "{:indent$}", "", indent = self.indent)?;
        self.node.explain(catalog, tx, f)?;
        writeln!(f)?;

        for child in self.node.children() {
            self.explain_child(Arc::clone(child)).explain(catalog, tx, f)?;
        }

        Ok(())
    }
}
