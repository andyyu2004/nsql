use std::error::Error;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use nsql::Nsql;
use nsql_core::schema::LogicalType;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner};

fn nsql_sqllogictest(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
    nsql_test::start(async {
        let db = TestDb(Nsql::mem().await.unwrap());
        let mut tester = Runner::new(db);
        tester.run_file_async(path).await?;
        Ok(())
    })
}

datatest_stable::harness!(
    nsql_sqllogictest,
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "tests/nsql-test/sqllogictest"),
    r"^.*/test_insert.slt"
);

// FIXME we need to test the single file pager too, but it's currently not `Send` due to `tokio_uring::File` not being send
pub struct TestDb(Nsql);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TypeWrapper(LogicalType);

impl ColumnType for TypeWrapper {
    fn from_char(value: char) -> Option<Self> {
        let ty = match value {
            'I' => LogicalType::Int,
            'b' => LogicalType::Bool,
            'd' => LogicalType::Decimal,
            _ => return None,
        };
        Some(TypeWrapper(ty))
    }

    fn to_char(&self) -> char {
        match self.0 {
            LogicalType::Int => 'I',
            LogicalType::Bool => 'B',
            LogicalType::Decimal => 'D',
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
