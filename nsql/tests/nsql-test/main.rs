use std::error::Error;

use nsql::Nsql;
use nsql_lmdb::LmdbStorageEngine;
use tracing_subscriber::EnvFilter;

mod explain;
mod opt;

fn nsql_debug_scratch(stmts: &str) -> nsql::Result<(), Box<dyn Error>> {
    let filter =
        EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG"));
    let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
    let db_path = nsql_test::tempfile::NamedTempFile::new()?.into_temp_path();

    let db = Nsql::<LmdbStorageEngine>::create(db_path).unwrap();
    let (conn, state) = db.connect();

    for stmt in stmts.lines() {
        let _ = conn.query(&state, stmt)?;
    }
    Ok(())
}

// copy whatever sql you want to run into `scratch.slt` and debug this test
#[test]
fn test_scratch_sqllogictest() -> nsql::Result<(), Box<dyn Error>> {
    let stmts = std::fs::read_to_string(format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/nsql-test/scratch.sql"
    ))?;
    nsql_debug_scratch(&stmts)
}
