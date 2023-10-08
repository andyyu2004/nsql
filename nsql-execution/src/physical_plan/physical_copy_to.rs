use std::fs::File;
use std::io::{self};
use std::sync::OnceLock;

use nsql_storage_engine::fallible_iterator;

use super::*;

pub(crate) struct PhysicalCopyTo<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> {
    children: PhysicalNodeId<'env, 'txn, S, M>,
    output_writer: OnceLock<File>, // particularly convenient to store a file for now as you can write with an `&File`
    dst: ir::CopyDestination,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> fmt::Debug
    for PhysicalCopyTo<'env, 'txn, S, M>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalCopyTo")
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>>
    PhysicalCopyTo<'env, 'txn, S, M>
{
    pub fn plan(
        source: PhysicalNodeId<'env, 'txn, S, M>,
        dst: ir::CopyDestination,
    ) -> Arc<dyn PhysicalNode<'env, 'txn, S, M>> {
        Arc::new(Self { dst, children: source, output_writer: Default::default() })
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalNode<'env, 'txn, S, M>
    for PhysicalCopyTo<'env, 'txn, S, M>
{
    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId<'env, 'txn, S, M>] {
        std::slice::from_ref(&self.children)
    }

    fn as_source(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSource<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }

    fn as_sink(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalSink<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Ok(self)
    }

    fn as_operator(
        self: Arc<Self>,
    ) -> Result<Arc<dyn PhysicalOperator<'env, 'txn, S, M>>, Arc<dyn PhysicalNode<'env, 'txn, S, M>>>
    {
        Err(self)
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSource<'env, 'txn, S, M>
    for PhysicalCopyTo<'env, 'txn, S, M>
{
    fn source(
        self: Arc<Self>,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
    ) -> ExecutionResult<TupleStream<'txn>> {
        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> PhysicalSink<'env, 'txn, S, M>
    for PhysicalCopyTo<'env, 'txn, S, M>
{
    fn sink(
        &self,
        _ecx: &'txn ExecutionContext<'_, 'env, S, M>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let out = self.output_writer.get_or_try_init::<_, io::Error>(|| {
            Ok(match &self.dst {
                ir::CopyDestination::File(path) => File::create(dbg!(path))?,
            })
        })?;

        let mut writer = csv::WriterBuilder::new().from_writer(out);
        writer.write_record(tuple.values().map(|tuple| match tuple {
            ir::Value::Null => "".into(),
            _ => tuple.to_string(),
        }))?;
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>> Explain<'env, S>
    for PhysicalCopyTo<'env, 'txn, S, M>
{
    fn as_dyn(&self) -> &dyn Explain<'env, S> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tx: &dyn Transaction<'env, S>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "copy to")?;
        Ok(())
    }
}
