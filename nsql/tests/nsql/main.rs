#![feature(never_type)]

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use nsql::Nsql;
use nsql_catalog::LogicalType;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner, TestError};
use walkdir::WalkDir;

#[test]
fn nsql_sqllogictest() -> nsql::Result<(), Vec<TestError>> {
    nsql_test::start(async {
        let mut errors = vec![];
        let db = TestDb(Nsql::mem().await.unwrap());
        let mut tester = Runner::new(db);
        let test_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/nsql/sqllogictest");
        for entry in WalkDir::new(test_path) {
            let entry = entry.unwrap();
            if entry.file_type().is_file() && entry.path().extension() == Some("slt".as_ref()) {
                if let Err(err) = tester.run_file_async(entry.path()).await {
                    errors.push(err);
                }
            }
        }

        if !errors.is_empty() { Err(errors) } else { Ok(()) }
    })
}

// FIXME we need to test the single file pager too, but it's currently not `Send` due to `tokio_uring::File` not being send
pub struct TestDb(Nsql);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TypeWrapper(LogicalType);

impl ColumnType for TypeWrapper {
    fn from_char(value: char) -> Option<Self> {
        let ty = match value {
            'i' => LogicalType::Int,
            _ => return None,
        };
        Some(TypeWrapper(ty))
    }

    fn to_char(&self) -> char {
        match self.0 {
            LogicalType::Int => 'i',
        }
    }
}

#[async_trait]
impl AsyncDB for TestDb {
    type Error = nsql::Error;

    type ColumnType = TypeWrapper;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let output = self.0.query(sql).await?;
        Ok(DBOutput::Rows {
            types: output.types.into_iter().map(TypeWrapper).collect(),
            rows: output
                .tuples
                .iter()
                .map(|t| t.values().iter().map(|v| v.to_string()).collect())
                .collect(),
        })
    }

    fn engine_name(&self) -> &str {
        "nsql"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
