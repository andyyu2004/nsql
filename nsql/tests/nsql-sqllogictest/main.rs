use std::error::Error;
use std::path::Path;

use ir::Value;
use nsql::{Connection, Nsql};
use nsql_core::LogicalType;
use nsql_lmdb::LmdbStorageEngine;
use nsql_redb::RedbStorageEngine;
use nsql_storage_engine::StorageEngine;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner};
use tracing_subscriber::EnvFilter;

fn nsql_sqllogictest(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
    fn test<S: StorageEngine>(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
        let filter =
            EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=INFO"));
        let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
        let db_path = tempfile::NamedTempFile::new()?.into_temp_path();
        tracing::info!(
            "Running test {} with {} on {}",
            path.display(),
            std::any::type_name::<S>(),
            db_path.display(),
        );
        let db = Nsql::<S>::create(db_path)?;
        let mut tester = Runner::new(|| async {
            let conn = db.connect();
            Ok(TestConnection { conn })
        });
        tester.with_hash_threshold(70);
        tester.run_file(path)?;
        Ok(())
    }

    test::<LmdbStorageEngine>(path)?;
    test::<RedbStorageEngine>(path)?;
    Ok(())
}

// This test isn't actually run as part of the test suite (due to custom harness),
// but it's useful for debugging the scratch file.
#[test]
fn test_scratch_sqllogictest() -> nsql::Result<(), Box<dyn Error>> {
    let stmts = std::fs::read_to_string(format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/nsql-sqllogictest/sqllogictest/scratch.slt"
    ))?;

    nsql_sqllogictest::<LmdbStorageEngine>(&stmts)?;
    Ok(())
}

datatest_stable::harness!(
    nsql_sqllogictest,
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "tests/nsql-sqllogictest/sqllogictest"),
    r"^.*/*.slt$",
    (ignore r"^.*/*.slow.slt$"),
);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TypeWrapper(LogicalType);

impl ColumnType for TypeWrapper {
    fn from_char(value: char) -> Option<Self> {
        let ty = match value {
            'I' => LogicalType::Int64,
            'B' => LogicalType::Bool,
            'D' => LogicalType::Decimal,
            'T' => LogicalType::Text,
            'A' => LogicalType::Array(Box::new(LogicalType::Int64)),
            'R' => LogicalType::Float64,
            _ => return None,
        };
        Some(TypeWrapper(ty))
    }

    fn to_char(&self) -> char {
        match self.0 {
            LogicalType::Int64 => 'I',
            LogicalType::Float64 => 'R',
            LogicalType::Bool => 'B',
            LogicalType::Decimal => 'D',
            LogicalType::Text => 'T',
            LogicalType::Null => todo!(),
            LogicalType::Oid => todo!(),
            LogicalType::Bytea => todo!(),
            LogicalType::Byte => todo!(),
            LogicalType::Array(_) => 'A',
            // pseudo types
            LogicalType::Type | LogicalType::TupleExpr | LogicalType::Expr | LogicalType::Any => {
                unreachable!()
            }
        }
    }
}

pub struct TestConnection<'env, S: StorageEngine> {
    conn: Connection<'env, S>,
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct ErrorWrapper(#[from] anyhow::Error);

#[async_trait::async_trait(?Send)]
impl<'env, S: StorageEngine> AsyncDB for TestConnection<'env, S> {
    type Error = ErrorWrapper;

    type ColumnType = TypeWrapper;

    #[tracing::instrument(skip(self))]
    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // transmute the lifetime back to whatever we need, not sure about safety on this one but it's a test so we'll find out
        let output = self.conn.query(sql)?;
        Ok(DBOutput::Rows {
            types: output.schema.into_iter().map(TypeWrapper).collect(),
            rows: output
                .tuples
                .iter()
                .map(|t| {
                    t.values()
                        .map(|v: &Value| match v {
                            Value::Text(s) => s.to_string(), // no quotes in the output
                            Value::Float64(f) => format!("{:.1}", f64::from_bits(*f)), // 1 dp
                            v => v.to_string(),
                        })
                        .collect()
                })
                .collect(),
        })
    }

    fn engine_name(&self) -> &str {
        "nsql"
    }
}
