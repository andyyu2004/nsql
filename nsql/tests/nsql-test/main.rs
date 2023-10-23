use std::error::Error;
use std::fs::File;
use std::path::Path;

use nsql::{MaterializedQueryOutput, Nsql};
use nsql_lmdb::LmdbStorageEngine;
use nsql_storage_engine::StorageEngine;
use tracing_subscriber::EnvFilter;

fn run_sql<S: StorageEngine>(sql: &str) -> nsql::Result<MaterializedQueryOutput> {
    let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
    let db = Nsql::<S>::create(db_path).unwrap();
    let conn = db.connect();
    conn.query(sql)
}

fn nsql_sqltest(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
    let filter =
        EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=INFO"));
    let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
    let sql = std::fs::read_to_string(path)?;
    let output = run_sql::<LmdbStorageEngine>(&sql)?;
    let expected_path = path.with_extension("expected");
    File::options().create(true).write(true).truncate(false).open(&expected_path)?;
    expect_test::expect_file![expected_path].assert_eq(&output.to_string());

    Ok(())
}

datatest_stable::harness!(
    nsql_sqltest,
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "tests/nsql-test/sqltest"),
    r"^.*/*.sql$",
);

// Copy whatever sql you want to run into `scratch.sql` and debug this test.
// This test won't run with `cargo test` as we have a custom harness.
#[test]
fn test_scratch_sql() -> nsql::Result<(), Box<dyn Error>> {
    use tracing_tree::HierarchicalLayer;
    let stmts = std::fs::read_to_string(format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/nsql-test/scratch.sql"
    ))?;

    fn nsql_debug_scratch<S: StorageEngine>(sql: &str) -> nsql::Result<(), Box<dyn Error>> {
        let subscriber = tracing_subscriber::registry()
            .with(
                EnvFilter::try_from_env("NSQL_LOG")
                    .unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG")),
            )
            .with(HierarchicalLayer::new(2));

        tracing::subscriber::with_default(subscriber, || {
            let output = run_sql(sql)?;
            eprintln!("{output}");

            Ok(())
        })
    }

    nsql_debug_scratch::<LmdbStorageEngine>(&stmts)?;
    nsql_debug_scratch::<RedbStorageEngine>(&stmts)
}
