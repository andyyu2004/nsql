use std::collections::HashSet;
use std::sync::Arc;

pub(crate) use nsql_arena::{Arena, Idx};

use crate::{PhysicalNode, PhysicalOperator, PhysicalSink, PhysicalSource};

pub(crate) struct MetaPipeline {
    pub(crate) sink: Arc<dyn PhysicalSink>,
    pub(crate) pipelines: Vec<Idx<Pipeline>>,
    pub(crate) children: Vec<Idx<MetaPipeline>>,
}

pub(crate) struct MetaPipelineBuilder {
    sink: Arc<dyn PhysicalSink>,
    pipelines: Vec<Idx<PipelineBuilder>>,
    children: Vec<Idx<MetaPipelineBuilder>>,
}

pub(crate) struct PipelineBuilderArena {
    root: Option<Idx<MetaPipelineBuilder>>,
    pipelines: Arena<PipelineBuilder>,
    meta_pipelines: Arena<MetaPipelineBuilder>,
}

impl PipelineBuilderArena {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink>) -> (Self, Idx<MetaPipelineBuilder>) {
        let mut builder =
            Self { pipelines: Default::default(), meta_pipelines: Default::default(), root: None };
        let root = MetaPipelineBuilder::new(&mut builder, sink);
        builder.root = Some(root);
        (builder, root)
    }

    pub(crate) fn finish(self) -> PipelineArena {
        let pipelines = self.pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let meta_pipelines =
            self.meta_pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let root = self.root.unwrap().cast();
        PipelineArena::new(root, pipelines, meta_pipelines)
    }
}

macro_rules! impl_index {
    ($name:ident . $field:ident: $idx:ident) => {
        impl std::ops::Index<Idx<$idx>> for $name {
            type Output = $idx;

            fn index(&self, index: Idx<$idx>) -> &Self::Output {
                &self.$field[index]
            }
        }

        impl std::ops::IndexMut<Idx<$idx>> for $name {
            fn index_mut(&mut self, index: Idx<$idx>) -> &mut Self::Output {
                &mut self.$field[index]
            }
        }
    };
}

impl_index!(PipelineBuilderArena.pipelines: PipelineBuilder);
impl_index!(PipelineBuilderArena.meta_pipelines: MetaPipelineBuilder);

pub(crate) struct PipelineArena {
    root: Idx<MetaPipeline>,
    pipelines: Arena<Pipeline>,
    meta_pipelines: Arena<MetaPipeline>,
}

impl PipelineArena {
    pub(crate) fn new(
        root: Idx<MetaPipeline>,
        pipelines: Arena<Pipeline>,
        meta_pipelines: Arena<MetaPipeline>,
    ) -> Self {
        let arena = Self { root, pipelines, meta_pipelines };
        arena.verify();
        arena
    }

    pub(crate) fn root(&self) -> Idx<MetaPipeline> {
        self.root
    }

    fn verify(&self) {
        self.verify_meta_pipeline(self.root);
    }

    fn verify_meta_pipeline(&self, idx: Idx<MetaPipeline>) {
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
            .map(|idx| Arc::as_ptr(&self[*idx].sink) as *const dyn PhysicalSource)
            .collect::<HashSet<_>>();

        assert!(child_meta_pipeline_sink_ptrs.is_subset(&pipeline_source_ptrs));

        for child in &meta_pipeline.children {
            self.verify_meta_pipeline(*child);
        }
    }
}

impl_index!(PipelineArena.pipelines: Pipeline);
impl_index!(PipelineArena.meta_pipelines: MetaPipeline);

impl MetaPipelineBuilder {
    pub(crate) fn new(arena: &mut PipelineBuilderArena, sink: Arc<dyn PhysicalSink>) -> Idx<Self> {
        arena.meta_pipelines.alloc(Self {
            pipelines: vec![arena.pipelines.alloc(PipelineBuilder::new(Arc::clone(&sink)))],
            sink,
            children: Default::default(),
        })
    }

    pub fn finish(self) -> MetaPipeline {
        let pipelines = self.pipelines.iter().map(|idx| idx.cast()).collect();
        let children = self.children.iter().map(|idx| idx.cast()).collect();
        MetaPipeline { sink: self.sink, pipelines, children }
    }
}

impl PipelineBuilderArena {
    pub(crate) fn new_child_meta_pipeline(
        &mut self,
        parent: Idx<MetaPipelineBuilder>,
        sink: Arc<dyn PhysicalSink>,
    ) -> Idx<MetaPipelineBuilder> {
        let child = MetaPipelineBuilder::new(self, sink);

        debug_assert!(
            !self[parent].children.contains(&child),
            "attempting to add duplicate child to meta pipeline"
        );
        self[parent].children.push(child);

        child
    }

    pub(crate) fn build(&mut self, idx: Idx<MetaPipelineBuilder>, node: Arc<dyn PhysicalNode>) {
        assert_eq!(self[idx].pipelines.len(), 1);
        assert!(self[idx].children.is_empty());
        node.build_pipelines(self, idx, self[idx].pipelines[0])
    }
}

pub struct Pipeline {
    pub(crate) source: Arc<dyn PhysicalSource>,
    /// The operators in the pipeline, ordered from source to sink.
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator>>,
    pub(crate) sink: Arc<dyn PhysicalSink>,
}

impl Pipeline {
    /// Returns an iterator over all nodes in the pipeline starting from the source and ending at the sink.
    pub(crate) fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn PhysicalNode> + '_ {
        std::iter::once(self.source.as_ref() as _)
            .chain(self.operators.iter().map(|op| op.as_ref() as _))
            .chain(Some(self.sink.as_ref() as _))
    }
}

pub(crate) struct PipelineBuilder {
    source: Option<Arc<dyn PhysicalSource>>,
    operators: Vec<Arc<dyn PhysicalOperator>>,
    sink: Arc<dyn PhysicalSink>,
}

impl PipelineBuilder {
    pub(crate) fn new(sink: Arc<dyn PhysicalSink>) -> Self {
        Self { source: None, operators: vec![], sink }
    }

    pub(crate) fn set_source(&mut self, source: Arc<dyn PhysicalSource>) {
        assert!(self.source.is_none(), "pipeline source already set");
        self.source = Some(source);
    }

    pub(crate) fn add_operator(&mut self, operator: Arc<dyn PhysicalOperator>) {
        self.operators.push(operator);
    }

    pub(crate) fn finish(mut self) -> Pipeline {
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
