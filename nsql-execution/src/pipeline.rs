use std::sync::Arc;

pub(crate) use crate::arena::{Arena, Idx};
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

#[derive(Default)]
pub(crate) struct PipelineBuilderArena {
    pipelines: Arena<PipelineBuilder>,
    meta_pipelines: Arena<MetaPipelineBuilder>,
}

impl PipelineBuilderArena {
    pub(crate) fn finish(self) -> PipelineArena {
        let pipelines = self.pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        let meta_pipelines =
            self.meta_pipelines.into_inner().into_iter().map(|p| p.finish()).collect();
        PipelineArena { pipelines, meta_pipelines }
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
    pipelines: Arena<Pipeline>,
    meta_pipelines: Arena<MetaPipeline>,
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

impl Idx<MetaPipelineBuilder> {
    pub(crate) fn new_child_meta_pipeline(
        self,
        arena: &mut PipelineBuilderArena,
        current: Idx<PipelineBuilder>,
        sink: Arc<dyn PhysicalSink>,
    ) -> Idx<MetaPipelineBuilder> {
        let child = MetaPipelineBuilder::new(arena, sink);
        arena[self].children.push(child);
        arena[self].pipelines.push(current);
        child
    }

    pub(crate) fn build(self, arena: &mut PipelineBuilderArena, node: Arc<dyn PhysicalNode>) {
        assert_eq!(arena[self].pipelines.len(), 1);
        assert!(arena[self].children.is_empty());
        node.build_pipelines(arena, arena[self].pipelines[0], self)
    }
}

pub struct Pipeline {
    pub(crate) source: Arc<dyn PhysicalSource>,
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator>>,
    pub(crate) sink: Arc<dyn PhysicalSink>,
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

    pub(crate) fn finish(self) -> Pipeline {
        Pipeline {
            source: self.source.expect("didn't provide source of pipeline to pipeline builder"),
            operators: self.operators,
            sink: self.sink,
        }
    }
}
