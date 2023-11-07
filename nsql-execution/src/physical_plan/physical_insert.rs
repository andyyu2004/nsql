use std::sync::OnceLock;

use nsql_catalog::{PrimaryKeyConflict, Table, TableStorage};
use nsql_core::Oid;
use nsql_storage::tuple::FromTuple;
use nsql_storage_engine::fallible_iterator;

use super::*;
use crate::ReadWriteExecutionMode;

#[derive(Debug)]
pub(crate) struct PhysicalInsert<'env, 'txn, S: StorageEngine> {
    id: PhysicalNodeId,
    children: [PhysicalNodeId; 1],
    table_oid: Oid<Table>,
    storage: Option<TableStorage<'env, 'txn, S, ReadWriteExecutionMode>>,
    table: OnceLock<Table>,
    returning: ExecutableTupleExpr<'env, 'txn, S, ReadWriteExecutionMode>,
    returning_tuples: Vec<Tuple>,
    evaluator: Evaluator,
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalInsert<'env, 'txn, S> {
    pub fn plan(
        table_oid: Oid<Table>,
        source: PhysicalNodeId,
        returning: ExecutableTupleExpr<'env, 'txn, S, ReadWriteExecutionMode>,
        arena: &mut PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> PhysicalNodeId {
        arena.alloc_with(|id| {
            Box::new(Self {
                id,
                table_oid,
                returning,
                children: [source],
                storage: Default::default(),
                table: Default::default(),
                returning_tuples: Default::default(),
                evaluator: Default::default(),
            })
        })
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalNode<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    impl_physical_node_conversions!(ReadWriteExecutionMode; source, sink; not operator);

    fn id(&self) -> PhysicalNodeId {
        self.id
    }

    fn width(&self, _nodes: &PhysicalNodeArena<'env, 'txn, S, ReadWriteExecutionMode>) -> usize {
        self.returning.width()
    }

    fn children(&self) -> &[PhysicalNodeId] {
        &self.children
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSink<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn sink(
        &mut self,
        ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode>,
        tuple: Tuple,
    ) -> ExecutionResult<()> {
        let catalog = ecx.catalog();
        let tx = ecx.tcx();

        let table = self.table.get_or_try_init(|| catalog.get(tx, self.table_oid))?;

        let storage = match &mut self.storage {
            Some(storage) => storage,
            None => {
                let storage = table.get_or_create_storage(catalog, tx)?;
                self.storage = Some(storage);
                self.storage.as_mut().unwrap()
            }
        };

        storage.insert(&catalog, tx, &tuple)?.map_err(|PrimaryKeyConflict { key }| {
            anyhow::anyhow!(
                "duplicate key `{key}` violates unique constraint in table `{}`",
                table.name(),
            )
        })?;

        if !self.returning.is_empty() {
            self.returning_tuples.push(self.returning.eval(
                &mut self.evaluator,
                catalog.storage(),
                tx,
                &tuple,
            )?);
        }

        // hack, if this is the insert of a `CREATE TABLE` we need to create the table storage
        if self.table_oid == Table::TABLE {
            let table = Table::from_tuple(tuple).expect("should be a compatible tuple");
            table.create_storage(catalog.storage(), tx.transaction())?;
        }

        Ok(())
    }

    fn finalize(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<()> {
        // drop the storage on finalization as it is no longer needed by this node
        // this helps avoids redb errors when the same table is opened by multiple nodes
        self.storage.take();

        Ok(())
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> PhysicalSource<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn source(
        &mut self,
        _ecx: &ExecutionContext<'_, 'env, 'txn, S, ReadWriteExecutionMode>,
    ) -> ExecutionResult<TupleStream<'_>> {
        let returning = std::mem::take(&mut self.returning_tuples);
        Ok(Box::new(fallible_iterator::convert(returning.into_iter().map(Ok))))
    }
}

impl<'env: 'txn, 'txn, S: StorageEngine> Explain<'env, 'txn, S, ReadWriteExecutionMode>
    for PhysicalInsert<'env, 'txn, S>
{
    fn as_dyn(&self) -> &dyn Explain<'env, 'txn, S, ReadWriteExecutionMode> {
        self
    }

    fn explain(
        &self,
        catalog: Catalog<'env, S>,
        tx: &dyn TransactionContext<'env, 'txn, S, ReadWriteExecutionMode>,
        f: &mut fmt::Formatter<'_>,
    ) -> explain::Result {
        write!(f, "insert into {}", catalog.table(tx, self.table_oid)?.name())?;
        Ok(())
    }
}
