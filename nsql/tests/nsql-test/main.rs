use std::error::Error;

use nsql::Nsql;
use nsql_lmdb::LmdbStorageEngine;
use nsql_redb::RedbStorageEngine;
use nsql_storage_engine::StorageEngine;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_tree::HierarchicalLayer;

mod explain;
mod explain_analyze;
mod opt;

fn nsql_debug_scratch<S: StorageEngine>(sql: &str) -> nsql::Result<(), Box<dyn Error>> {
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG")))
        .with(HierarchicalLayer::new(2));

    tracing::subscriber::with_default(subscriber, || {
        let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
        let db = Nsql::<S>::create(db_path).unwrap();
        let (conn, state) = db.connect();
        let output = conn.query(&state, sql)?;
        eprintln!("{output}");

        Ok(())
    })
}

// copy whatever sql you want to run into `scratch.sql` and debug this test
#[test]
fn test_scratch_sql() -> nsql::Result<(), Box<dyn Error>> {
    let stmts = std::fs::read_to_string(format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/nsql-test/scratch.sql"
    ))?;

    nsql_debug_scratch::<LmdbStorageEngine>(&stmts)?;
    nsql_debug_scratch::<RedbStorageEngine>(&stmts)
}
