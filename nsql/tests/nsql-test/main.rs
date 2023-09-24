use std::error::Error;

use nsql::Nsql;
use nsql_lmdb::LmdbStorageEngine;
use nsql_redb::RedbStorageEngine;
use nsql_storage_engine::StorageEngine;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;
use tracing_tree::HierarchicalLayer;

mod explain;
mod opt;

fn nsql_debug_scratch<S: StorageEngine>(sql: &str) -> nsql::Result<(), Box<dyn Error>> {
    // let filter =
    //     EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG"));
    // let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
    // let _ = tracing_subscriber::fmt::init();
    let subscriber = Registry::default().with(HierarchicalLayer::new(2));
    tracing::subscriber::with_default(subscriber, || {
        let db_path = nsql_test::tempfile::NamedTempFile::new()?.into_temp_path();
        let db = Nsql::<S>::create(db_path).unwrap();
        let (conn, state) = db.connect();
        let output = conn.query(&state, sql)?;
        eprintln!("{output:?}");

        Ok(())
    })
}

// copy whatever sql you want to run into `scratch.slt` and debug this test
#[test]
fn test_scratch_sqllogictest() -> nsql::Result<(), Box<dyn Error>> {
    let stmts = std::fs::read_to_string(format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/nsql-test/scratch.sql"
    ))?;

    nsql_debug_scratch::<LmdbStorageEngine>(&stmts)?;
    nsql_debug_scratch::<RedbStorageEngine>(&stmts)
}
