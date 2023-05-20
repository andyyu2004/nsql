use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use nsql_arena::{Arena, Idx};
use nsql_storage_engine::StorageEngine;

use crate::{ExecutionMode, PhysicalNode, PhysicalOperator, PhysicalSink, PhysicalSource};

pub(crate) struct MetaPipeline<S, M> {
    pub(crate) sink: Arc<dyn PhysicalSink<S, M>>,
    pub(crate) pipelines: Vec<Idx<Pipeline<S, M>>>,
    pub(crate) children: Vec<Idx<MetaPipeline<S, M>>>,
}

pub(crate) struct MetaPipelineBuilder<S, M> {
    sink: Arc<dyn PhysicalSink<S, M>>,
    pipelines: Vec<Idx<PipelineBuilder<S, M>>>,
    children: Vec<Idx<MetaPipelineBuilder<S, M>>>,
}

pub(crate) struct PipelineBuilderArena<S, M> {
    root: Option<Idx<MetaPipelineBuilder<S, M>>>,
    pipelines: Arena<PipelineBuilder<S, M>>,
    meta_pipelines: Arena<MetaPipelineBuilder<S, M>>,
}

impl<S, M> PipelineBuilderArena<S, M> {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink<S, M>>) -> (Self, Idx<MetaPipelineBuilder<S, M>>) {
        let mut builder =
            Self { pipelines: Default::default(), meta_pipelines: Default::default(), root: None };
        let root = MetaPipelineBuilder::new(&mut builder, sink);
        builder.root = Some(root);
        (builder, root)
    }

    pub(crate) fn finish(self) -> PipelineArena<S, M> {
        let pipelines = self.pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let meta_pipelines =
            self.meta_pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let root = self.root.unwrap().cast();
        PipelineArena::new(root, pipelines, meta_pipelines)
    }
}

macro_rules! impl_index {
    ($name:ident . $field:ident: $idx:ident) => {
        impl<S, M> std::ops::Index<Idx<$idx<S, M>>> for $name<S, M> {
            type Output = $idx<S, M>;

            #[inline]
            fn index(&self, index: Idx<$idx<S, M>>) -> &Self::Output {
                &self.$field[index]
            }
        }

        impl<S, M> std::ops::IndexMut<Idx<$idx<S, M>>> for $name<S, M> {
            #[inline]
            fn index_mut(&mut self, index: Idx<$idx<S, M>>) -> &mut Self::Output {
                &mut self.$field[index]
            }
        }
    };
}

impl_index!(PipelineBuilderArena.pipelines: PipelineBuilder);
impl_index!(PipelineBuilderArena.meta_pipelines: MetaPipelineBuilder);

pub(crate) struct PipelineArena<S, M> {
    root: Idx<MetaPipeline<S, M>>,
    pipelines: Arena<Pipeline<S, M>>,
    meta_pipelines: Arena<MetaPipeline<S, M>>,
}

impl<S, M> PipelineArena<S, M> {
    pub(crate) fn new(
        root: Idx<MetaPipeline<S, M>>,
        pipelines: Arena<Pipeline<S, M>>,
        meta_pipelines: Arena<MetaPipeline<S, M>>,
    ) -> Self {
        let arena = Self { root, pipelines, meta_pipelines };
        arena.verify();
        arena
    }

    pub(crate) fn root(&self) -> Idx<MetaPipeline<S, M>> {
        self.root
    }

    fn verify(&self) {
        self.verify_meta_pipeline(self.root);
    }

    fn verify_meta_pipeline(&self, idx: Idx<MetaPipeline<S, M>>) {
        let meta_pipeline = &self.meta_pipelines[idx];

        // check that all child_meta_pipeline sinks are the source of some pipeline
        let pipeline_source_ptrs = meta_pipeline
            .pipelines
            .iter()
            .map(|idx| Arc::as_ptr(&self[*idx].source))
            .collect::<HashSet<_>>();

        let child_meta_pipeline_sink_ptrs = meta_pipeline
            .children
            .iter()
            .map(|idx| Arc::as_ptr(&self[*idx].sink) as *const dyn PhysicalSource<S, M>)
            .collect::<HashSet<_>>();

        assert!(child_meta_pipeline_sink_ptrs.is_subset(&pipeline_source_ptrs));

        for child in &meta_pipeline.children {
            self.verify_meta_pipeline(*child);
        }
    }
}

impl_index!(PipelineArena.pipelines: Pipeline);
impl_index!(PipelineArena.meta_pipelines: MetaPipeline);

impl<S, M> MetaPipelineBuilder<S, M> {
    pub(crate) fn new(
        arena: &mut PipelineBuilderArena<S, M>,
        sink: Arc<dyn PhysicalSink<S, M>>,
    ) -> Idx<Self> {
        arena.meta_pipelines.alloc(Self {
            pipelines: vec![arena.pipelines.alloc(PipelineBuilder::new(Arc::clone(&sink)))],
            sink,
            children: Default::default(),
        })
    }

    pub fn finish(self) -> MetaPipeline<S, M> {
        let pipelines = self.pipelines.iter().map(|idx| idx.cast()).collect();
        let children = self.children.iter().map(|idx| idx.cast()).collect();
        MetaPipeline { sink: self.sink, pipelines, children }
    }
}

impl<S: StorageEngine, M: ExecutionMode<S>> PipelineBuilderArena<S, M> {
    pub(crate) fn new_child_meta_pipeline(
        &mut self,
        parent: Idx<MetaPipelineBuilder<S, M>>,
        sink: Arc<dyn PhysicalSink<S, M>>,
    ) -> Idx<MetaPipelineBuilder<S, M>> {
        let child = MetaPipelineBuilder::new(self, sink);

        debug_assert!(
            !self[parent].children.contains(&child),
            "attempting to add duplicate child to meta pipeline"
        );
        self[parent].children.push(child);

        child
    }

    pub(crate) fn build(
        &mut self,
        idx: Idx<MetaPipelineBuilder<S, M>>,
        node: Arc<dyn PhysicalNode<S, M>>,
    ) {
        assert_eq!(self[idx].pipelines.len(), 1);
        assert!(self[idx].children.is_empty());
        node.build_pipelines(self, idx, self[idx].pipelines[0])
    }
}

pub struct Pipeline<S, M> {
    pub(crate) source: Arc<dyn PhysicalSource<S, M>>,
    /// The operators in the pipeline, ordered from source to sink.
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator<S, M>>>,
    pub(crate) sink: Arc<dyn PhysicalSink<S, M>>,
}

impl<S, M> Pipeline<S, M> {
    /// Returns an iterator over all nodes in the pipeline starting from the source and ending at the sink.
    pub(crate) fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn PhysicalNode<S, M>> + '_ {
        std::iter::once(self.source.as_ref() as _)
            .chain(self.operators.iter().map(|op| op.as_ref() as _))
            .chain(Some(self.sink.as_ref() as _))
    }
}

pub(crate) struct PipelineBuilder<S, M> {
    source: Option<Arc<dyn PhysicalSource<S, M>>>,
    operators: Vec<Arc<dyn PhysicalOperator<S, M>>>,
    sink: Arc<dyn PhysicalSink<S, M>>,
}

impl<S, M> PipelineBuilder<S, M> {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink<S, M>>) -> Self {
        Self { source: None, operators: vec![], sink }
    }

    pub(crate) fn set_source(&mut self, source: Arc<dyn PhysicalSource<S, M>>) {
        assert!(self.source.is_none(), "pipeline source already set");
        self.source = Some(source);
    }

    pub(crate) fn add_operator(&mut self, operator: Arc<dyn PhysicalOperator<S, M>>) {
        self.operators.push(operator);
    }

    pub(crate) fn finish(mut self) -> Pipeline<S, M> {
        // The order in which operators are added to the pipeline builder is the reverse of the execution order.
        // We add operators top down (relative to the physical plan tree) due to the pipeline building algorithm, but we need to execute the operators bottom up.
        self.operators.reverse();
        Pipeline {
            source: self.source.expect("did not provide source of pipeline to pipeline builder"),
            operators: self.operators,
            sink: self.sink,
        }
    }
}
