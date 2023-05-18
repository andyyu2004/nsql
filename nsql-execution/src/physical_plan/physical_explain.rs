use atomic_take::AtomicTake;
use nsql_storage::value::Value;

use super::*;

#[derive(Debug)]
pub struct PhysicalExplain {
    stringified_plan: AtomicTake<String>,
    children: [Arc<dyn PhysicalNode<S>>; 1],
}

impl PhysicalExplain {
    #[inline]
    pub(crate) fn plan(
        stringified_plan: String,
        child: Arc<dyn PhysicalNode<S>>,
    ) -> Arc<dyn PhysicalNode<S>> {
        Arc::new(Self { stringified_plan: AtomicTake::new(stringified_plan), children: [child] })
    }
}

impl PhysicalNode<S> for PhysicalExplain {
    fn children(&self) -> &[Arc<dyn PhysicalNode<S>>] {
        // no children as we don't actually need to run anything (unless we're doing an analyse which is not implemented)
        &[]
    }

    fn as_source(self: Arc<Self>) -> Result<Arc<dyn PhysicalSource<S>>, Arc<dyn PhysicalNode<S>>> {
        Ok(self)
    }

    fn as_sink(self: Arc<Self>) -> Result<Arc<dyn PhysicalSink<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<S>>, Arc<dyn PhysicalNode<S>>> {
        Err(self)
    }
}

#[async_trait::async_trait]
impl PhysicalSource for PhysicalExplain {
    async fn source(&self, _ctx: &ExecutionContext) -> ExecutionResult<SourceState<Chunk>> {
        let plan = self.stringified_plan.take().expect("should not be called again");
        Ok(SourceState::Final(Chunk::singleton(Tuple::from(vec![Value::Text(plan)]))))
    }
}

impl Explain for PhysicalExplain {
    fn explain(
        &self,
        _catalog: &Catalog,
        _tx: &Transaction,
        _f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        unreachable!("cannot explain an explain node")
    }
}
