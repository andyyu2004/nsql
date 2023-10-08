pub(crate) use nsql_arena::{Arena, Idx};
use nsql_storage_engine::StorageEngine;

use crate::{
    ExecutionMode, PhysicalNodeArena, PhysicalNodeId, PhysicalOperator, PhysicalSink,
    PhysicalSource,
};

pub(crate) struct RootPipeline<'env, 'txn, S, M> {
    pub arena: PipelineArena<'env, 'txn, S, M>,
    pub nodes: PhysicalNodeArena<'env, 'txn, S, M>,
}

impl<'env, 'txn, S, M> RootPipeline<'env, 'txn, S, M> {
    pub fn new(
        arena: PipelineArena<'env, 'txn, S, M>,
        nodes: PhysicalNodeArena<'env, 'txn, S, M>,
    ) -> Self {
        Self { arena, nodes }
    }
}

pub(crate) struct MetaPipeline<'env, 'txn, S, M> {
    pub(crate) pipelines: Vec<Idx<Pipeline<'env, 'txn, S, M>>>,
    pub(crate) children: Vec<Idx<MetaPipeline<'env, 'txn, S, M>>>,
}

#[derive(Debug)]
pub(crate) struct MetaPipelineBuilder<'env, 'txn, S, M> {
    pipelines: Vec<Idx<PipelineBuilder<'env, 'txn, S, M>>>,
    children: Vec<Idx<MetaPipelineBuilder<'env, 'txn, S, M>>>,
}

#[derive(Debug)]
pub(crate) struct PipelineBuilderArena<'env, 'txn, S, M> {
    root: Option<Idx<MetaPipelineBuilder<'env, 'txn, S, M>>>,
    pipelines: Arena<PipelineBuilder<'env, 'txn, S, M>>,
    meta_pipelines: Arena<MetaPipelineBuilder<'env, 'txn, S, M>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PipelineBuilderArena<'env, 'txn, S, M>
{
    pub(crate) fn new(
        sink: &dyn PhysicalSink<'env, 'txn, S, M>,
    ) -> (Self, Idx<MetaPipelineBuilder<'env, 'txn, S, M>>) {
        let mut builder =
            Self { pipelines: Default::default(), meta_pipelines: Default::default(), root: None };
        let root = MetaPipelineBuilder::new(&mut builder, sink);
        builder.root = Some(root);
        (builder, root)
    }

    pub(crate) fn finish(self) -> PipelineArena<'env, 'txn, S, M> {
        let pipelines = self.pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let meta_pipelines =
            self.meta_pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let root = self.root.unwrap().cast();
        PipelineArena::new(root, pipelines, meta_pipelines)
    }
}

macro_rules! impl_index {
    ($name:ident . $field:ident: $idx:ident) => {
        impl<'env, 'txn, S, M> std::ops::Index<Idx<$idx<'env, 'txn, S, M>>>
            for $name<'env, 'txn, S, M>
        {
            type Output = $idx<'env, 'txn, S, M>;

            #[inline]
            fn index(&self, index: Idx<$idx<'env, 'txn, S, M>>) -> &Self::Output {
                &self.$field[index]
            }
        }

        impl<'env, 'txn, S, M> std::ops::IndexMut<Idx<$idx<'env, 'txn, S, M>>>
            for $name<'env, 'txn, S, M>
        {
            #[inline]
            fn index_mut(&mut self, index: Idx<$idx<'env, 'txn, S, M>>) -> &mut Self::Output {
                &mut self.$field[index]
            }
        }
    };
}

impl_index!(PipelineBuilderArena.pipelines: PipelineBuilder);
impl_index!(PipelineBuilderArena.meta_pipelines: MetaPipelineBuilder);

pub(crate) struct PipelineArena<'env, 'txn, S, M> {
    root: Idx<MetaPipeline<'env, 'txn, S, M>>,
    pipelines: Arena<Pipeline<'env, 'txn, S, M>>,
    meta_pipelines: Arena<MetaPipeline<'env, 'txn, S, M>>,
}

impl<'env, 'txn, S, M> PipelineArena<'env, 'txn, S, M> {
    pub(crate) fn new(
        root: Idx<MetaPipeline<'env, 'txn, S, M>>,
        pipelines: Arena<Pipeline<'env, 'txn, S, M>>,
        meta_pipelines: Arena<MetaPipeline<'env, 'txn, S, M>>,
    ) -> Self {
        let arena = Self { root, pipelines, meta_pipelines };
        arena.verify();
        arena
    }

    pub(crate) fn root(&self) -> Idx<MetaPipeline<'env, 'txn, S, M>> {
        self.root
    }

    fn verify(&self) {
        self.verify_meta_pipeline(self.root);
    }

    fn verify_meta_pipeline(&self, idx: Idx<MetaPipeline<'env, 'txn, S, M>>) {
        // No logic here at the moment
        let meta_pipeline = &self.meta_pipelines[idx];

        for child in &meta_pipeline.children {
            self.verify_meta_pipeline(*child);
        }
    }
}

impl_index!(PipelineArena.pipelines: Pipeline);
impl_index!(PipelineArena.meta_pipelines: MetaPipeline);

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    MetaPipelineBuilder<'env, 'txn, S, M>
{
    pub(crate) fn new(
        arena: &mut PipelineBuilderArena<'env, 'txn, S, M>,
        sink: &dyn PhysicalSink<'env, 'txn, S, M>,
    ) -> Idx<Self> {
        arena.meta_pipelines.alloc(Self {
            pipelines: vec![arena.pipelines.alloc(PipelineBuilder::new(sink))],
            children: Default::default(),
        })
    }

    pub fn finish(self) -> MetaPipeline<'env, 'txn, S, M> {
        let pipelines = self.pipelines.iter().map(|idx| idx.cast()).collect();
        let children = self.children.iter().map(|idx| idx.cast()).collect();
        MetaPipeline { pipelines, children }
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PipelineBuilderArena<'env, 'txn, S, M>
{
    pub(crate) fn new_child_meta_pipeline(
        &mut self,
        parent: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        sink: &dyn PhysicalSink<'env, 'txn, S, M>,
    ) -> Idx<MetaPipelineBuilder<'env, 'txn, S, M>> {
        let child = MetaPipelineBuilder::new(self, sink);

        debug_assert!(
            !self[parent].children.contains(&child),
            "attempting to add duplicate child to meta pipeline"
        );
        self[parent].children.push(child);

        child
    }

    #[track_caller]
    pub(crate) fn build(
        &mut self,
        nodes: &PhysicalNodeArena<'env, 'txn, S, M>,
        meta_pipeline: Idx<MetaPipelineBuilder<'env, 'txn, S, M>>,
        node: PhysicalNodeId<'env, 'txn, S, M>,
    ) {
        assert_eq!(self[meta_pipeline].pipelines.len(), 1);
        assert!(self[meta_pipeline].children.is_empty());
        nodes[node].clone().build_pipelines(
            nodes,
            self,
            meta_pipeline,
            self[meta_pipeline].pipelines[0],
        )
    }
}

pub struct Pipeline<'env, 'txn, S, M> {
    pub(crate) source: PhysicalNodeId<'env, 'txn, S, M>,
    /// The operators in the pipeline, ordered from source to sink.
    pub(crate) operators: Box<[PhysicalNodeId<'env, 'txn, S, M>]>,
    pub(crate) sink: PhysicalNodeId<'env, 'txn, S, M>,
}

impl<'env, 'txn, S, M> Pipeline<'env, 'txn, S, M> {
    /// Returns an iterator over all nodes in the pipeline starting from the source and ending at the sink.
    pub(crate) fn nodes(
        &self,
    ) -> impl DoubleEndedIterator<Item = PhysicalNodeId<'env, 'txn, S, M>> + '_ {
        std::iter::once(self.source as _)
            .chain(self.operators.iter().copied())
            .chain(Some(self.sink))
    }
}

#[derive(Debug)]
pub(crate) struct PipelineBuilder<'env, 'txn, S, M> {
    source: Option<PhysicalNodeId<'env, 'txn, S, M>>,
    operators: Vec<PhysicalNodeId<'env, 'txn, S, M>>,
    sink: PhysicalNodeId<'env, 'txn, S, M>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PipelineBuilder<'env, 'txn, S, M> {
    // NOTE: we require these methods to require the trait object even though we only need the id for type safety.
    // The id doesn't tell us which of the 3 traits are implemented.
    pub(crate) fn new(sink: &dyn PhysicalSink<'env, 'txn, S, M>) -> Self {
        Self { source: None, operators: vec![], sink: sink.id() }
    }

    #[track_caller]
    pub(crate) fn set_source(&mut self, source: &dyn PhysicalSource<'env, 'txn, S, M>) {
        assert!(self.source.is_none(), "pipeline source already set");
        self.source = Some(source.id());
    }

    pub(crate) fn add_operator(&mut self, operator: &dyn PhysicalOperator<'env, 'txn, S, M>) {
        self.operators.push(operator.id());
    }

    pub(crate) fn finish(mut self) -> Pipeline<'env, 'txn, S, M> {
        // The order in which operators are added to the pipeline builder is the reverse of the execution order.
        // We add operators top down (relative to the physical plan tree) due to the pipeline building algorithm, but we need to execute the operators bottom up.
        self.operators.reverse();
        Pipeline {
            source: self.source.expect("did not provide source of pipeline to pipeline builder"),
            operators: self.operators.into_boxed_slice(),
            sink: self.sink,
        }
    }
}
