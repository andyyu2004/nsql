use std::sync::Arc;

use crate::{PhysicalOperator, PhysicalSource};

pub struct Pipeline {
    pub(crate) source: Arc<dyn PhysicalSource>,
    pub(crate) operators: Vec<Arc<dyn PhysicalOperator>>,
}

#[derive(Default)]
pub(crate) struct PipelineBuilder {
    source: Option<Arc<dyn PhysicalSource>>,
    operators: Vec<Arc<dyn PhysicalOperator>>,
}

impl PipelineBuilder {
    pub(crate) fn set_source(&mut self, source: Arc<dyn PhysicalSource>) {
        self.source = Some(source);
    }

    pub(crate) fn add_operator(&mut self, operator: Arc<dyn PhysicalOperator>) {
        self.operators.push(operator);
    }

    pub(crate) fn build(self) -> Pipeline {
        Pipeline {
            source: self.source.expect("didn't provide source of pipeline to pipeline builder"),
            operators: self.operators,
        }
    }
}
