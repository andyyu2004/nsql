use std::fs::File;
use std::io::{self};
use std::marker::PhantomData;
use std::sync::OnceLock;

use nsql_storage_engine::fallible_iterator;

use super::*;

pub(crate) struct PhysicalCopyTo<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
{
    id: PhysicalNodeId,
    children: PhysicalNodeId,
    output_writer: OnceLock<File>,
    // particularly convenient to store a file for now as you can write with an `&File`
    // now that we have mutable access we can hold a `Box<dyn Write>` or something
    dst: ir::CopyDestination,
    _marker: PhantomData<dyn PhysicalNode<'env, 'txn, S, M, T>>,
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple> fmt::Debug
    for PhysicalCopyTo<'env, 'txn, S, M, T>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalCopyTo")
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalCopyTo<'env, 'txn, S, M, T>
{
    pub fn plan(
        source: PhysicalNodeId,
        dst: ir::CopyDestination,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, M, T>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                dst,
                children: source,
                output_writer: Default::default(),
                _marker: PhantomData,
            })
        })
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalNode<'env, 'txn, S, M, T> for PhysicalCopyTo<'env, 'txn, S, M, T>
{
    impl_physical_node_conversions!(M; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, M, T>) -> usize {
        0
    }

    fn children(&self) -> &[PhysicalNodeId] {
        std::slice::from_ref(&self.children)
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSource<'env, 'txn, S, M, T> for PhysicalCopyTo<'env, 'txn, S, M, T>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
    ) -> ExecutionResult<TupleStream<'_, T>> {
        Ok(Box::new(fallible_iterator::empty()))
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple>
    PhysicalSink<'env, 'txn, S, M, T> for PhysicalCopyTo<'env, 'txn, S, M, T>
{
    fn sink(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, M, T>,
        tuple: T,
    ) -> ExecutionResult<()> {
        let out = self.output_writer.get_or_try_init::<_, io::Error>(|| {
            Ok(match &self.dst {
                ir::CopyDestination::File(path) => File::create(path)?,
            })
        })?;

        let mut writer = csv::WriterBuilder::new().from_writer(out);
        writer.write_record(tuple.values().map(|value| match value {
            ir::Value::Null => "".into(),
            _ => value.to_string(),
        }))?;
        Ok(())
    }
}

impl<'env, 'txn, S: StorageEngine, M: ExecutionMode<'env, S>, T: Tuple> Explain<'env, 'txn, S, M>
    for PhysicalCopyTo<'env, 'txn, S, M, T>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, M> {
        self
    }

    fn explain(
        &self,
        _catalog: Catalog<'env, S>,
        _tcx: &dyn TransactionContext<'env, '_, S, M>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "copy to")?;
        Ok(())
    }
}
