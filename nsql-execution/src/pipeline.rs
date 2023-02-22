use crate::{PhysicalOperator, PhysicalSource};

pub struct Pipeline {
    source: Box<dyn PhysicalSource>,
    operators: Box<dyn PhysicalOperator>,
}
