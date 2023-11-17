use anyhow::anyhow;
use nsql_redb::RedbStorageEngine;
use nsql_storage::tuple::FlatTuple;
use nsql_storage_engine::{ReadonlyExecutionMode, StorageEngine};

use super::*;

#[test]
fn smoke() -> Result<()> {
    let mut module = Module::new()?;
    type S = RedbStorageEngine;
    let storage = S::create("/tmp/test-exec-tmp-todo")?;
    let profiler = Profiler::new("/tmp/profiler").map_err(|err| anyhow!(err))?;
    let catalog = Catalog::new(&storage);
    let expected_literal = false;
    let expr = ExecutableExpr::<S, ReadonlyExecutionMode>::literal(expected_literal);
    let f = module.compile(&expr)?;
    let tuple = FlatTuple::default();
    let (t, v) = tuple.to_raw_parts();

    let mut out = value::Value::Null;
    assert_eq!((f.ptr)(catalog.storage(), &profiler, 0, 0, t, v, &mut out), 0);
    assert_eq!(out, expected_literal.into());
    Ok(())
}
