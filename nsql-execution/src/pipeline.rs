use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use nsql_arena::{Arena, Idx};

use crate::{PhysicalNode, PhysicalOperator, PhysicalSink, PhysicalSource};

pub(crate) struct MetaPipeline<S> {
    pub(crate) sink: Arc<dyn PhysicalSink<S>>,
    pub(crate) pipelines: Vec<Idx<Pipeline<S>>>,
    pub(crate) children: Vec<Idx<MetaPipeline<S>>>,
}

pub(crate) struct MetaPipelineBuilder<S> {
    sink: Arc<dyn PhysicalSink<S>>,
    pipelines: Vec<Idx<PipelineBuilder<S>>>,
    children: Vec<Idx<MetaPipelineBuilder<S>>>,
}

pub(crate) struct PipelineBuilderArena<S> {
    root: Option<Idx<MetaPipelineBuilder<S>>>,
    pipelines: Arena<PipelineBuilder<S>>,
    meta_pipelines: Arena<MetaPipelineBuilder<S>>,
}

impl<S> PipelineBuilderArena<S> {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink<S>>) -> (Self, Idx<MetaPipelineBuilder<S>>) {
        let mut builder =
            Self { pipelines: Default::default(), meta_pipelines: Default::default(), root: None };
        let root = MetaPipelineBuilder::new(&mut builder, sink);
        builder.root = Some(root);
        (builder, root)
    }

    pub(crate) fn finish(self) -> PipelineArena<S> {
        let pipelines = self.pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let meta_pipelines =
            self.meta_pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let root = self.root.unwrap().cast();
        PipelineArena::new(root, pipelines, meta_pipelines)
    }
}

macro_rules! impl_index {
    ($name:ident . $field:ident: $idx:ident) => {
        impl<S> std::ops::Index<Idx<$idx<S>>> for $name<S> {
            type Output = $idx<S>;

            #[inline]
            fn index(&self, index: Idx<$idx<S>>) -> &Self::Output {
                &self.$field[index]
            }
        }

        impl<S> std::ops::IndexMut<Idx<$idx<S>>> for $name<S> {
            #[inline]
            fn index_mut(&mut self, index: Idx<$idx<S>>) -> &mut Self::Output {
                &mut self.$field[index]
            }
        }
    };
}

impl_index!(PipelineBuilderArena.pipelines: PipelineBuilder);
impl_index!(PipelineBuilderArena.meta_pipelines: MetaPipelineBuilder);

pub(crate) struct PipelineArena<S> {
    root: Idx<MetaPipeline<S>>,
    pipelines: Arena<Pipeline<S>>,
    meta_pipelines: Arena<MetaPipeline<S>>,
}

impl<S> PipelineArena<S> {
    pub(crate) fn new(
        root: Idx<MetaPipeline<S>>,
        pipelines: Arena<Pipeline<S>>,
        meta_pipelines: Arena<MetaPipeline<S>>,
    ) -> Self {
        let arena = Self { root, pipelines, meta_pipelines };
        arena.verify();
        arena
    }

    pub(crate) fn root(&self) -> Idx<MetaPipeline<S>> {
        self.root
    }

    fn verify(&self) {
        self.verify_meta_pipeline(self.root);
    }

    fn verify_meta_pipeline(&self, idx: Idx<MetaPipeline<S>>) {
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
            .map(|idx| Arc::as_ptr(&self[*idx].sink) as *const dyn PhysicalSource<S>)
            .collect::<HashSet<_>>();

        assert!(child_meta_pipeline_sink_ptrs.is_subset(&pipeline_source_ptrs));

        for child in &meta_pipeline.children {
            self.verify_meta_pipeline(*child);
        }
    }
}

impl_index!(PipelineArena.pipelines: Pipeline);
impl_index!(PipelineArena.meta_pipelines: MetaPipeline);

impl<S> MetaPipelineBuilder<S> {
    pub(crate) fn new(
        arena: &mut PipelineBuilderArena<S>,
        sink: Arc<dyn PhysicalSink<S>>,
    ) -> Idx<Self> {
        arena.meta_pipelines.alloc(Self {
            pipelines: vec![arena.pipelines.alloc(PipelineBuilder::new(Arc::clone(&sink)))],
            sink,
            children: Default::default(),
        })
    }

    pub fn finish(self) -> MetaPipeline<S> {
        let pipelines = self.pipelines.iter().map(|idx| idx.cast()).collect();
        let children = self.children.iter().map(|idx| idx.cast()).collect();
        MetaPipeline { sink: self.sink, pipelines, children }
    }
}

impl<S> PipelineBuilderArena<S> {
    pub(crate) fn new_child_meta_pipeline(
        &mut self,
        parent: Idx<MetaPipelineBuilder<S>>,
        sink: Arc<dyn PhysicalSink<S>>,
    ) -> Idx<MetaPipelineBuilder<S>> {
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
        idx: Idx<MetaPipelineBuilder<S>>,
        node: Arc<dyn PhysicalNode<S>>,
    ) {
        assert_eq!(self[idx].pipelines.len(), 1);
        assert!(self[idx].children.is_empty());
        node.build_pipelines(self, idx, self[idx].pipelines[0])
    }
}

pub struct Pipeline<S> {
    pub(crate) source: Arc<dyn PhysicalSource<S>>,
    /// The operators in the pipeline, ordered from source to sink.
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator<S>>>,
    pub(crate) sink: Arc<dyn PhysicalSink<S>>,
}

impl<S> Pipeline<S> {
    /// Returns an iterator over all nodes in the pipeline starting from the source and ending at the sink.
    pub(crate) fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn PhysicalNode<S>> + '_ {
        std::iter::once(self.source.as_ref() as _)
            .chain(self.operators.iter().map(|op| op.as_ref() as _))
            .chain(Some(self.sink.as_ref() as _))
    }
}

pub(crate) struct PipelineBuilder<S> {
    source: Option<Arc<dyn PhysicalSource<S>>>,
    operators: Vec<Arc<dyn PhysicalOperator<S>>>,
    sink: Arc<dyn PhysicalSink<S>>,
}

impl<S> PipelineBuilder<S> {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink<S>>) -> Self {
        Self { source: None, operators: vec![], sink }
    }

    pub(crate) fn set_source(&mut self, source: Arc<dyn PhysicalSource<S>>) {
        assert!(self.source.is_none(), "pipeline source already set");
        self.source = Some(source);
    }

    pub(crate) fn add_operator(&mut self, operator: Arc<dyn PhysicalOperator<S>>) {
        self.operators.push(operator);
    }

    pub(crate) fn finish(mut self) -> Pipeline<S> {
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
