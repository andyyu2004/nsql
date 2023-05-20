use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use nsql::{Connection, Nsql};
use nsql_storage::schema::LogicalType;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner};
use tracing_subscriber::EnvFilter;

fn nsql_sqllogictest(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
    nsql_test::start(async {
        let filter =
            EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG"));
        let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
        let db = TestDb::new(Nsql::mem().await.unwrap());
        let mut tester = Runner::new(db);
        tester.run_file_async(path).await?;
        Ok(())
    })
}

// if you wish to debug a particular test, you can temporarily change the regex to match only that test
datatest_stable::harness!(
    nsql_sqllogictest,
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), "tests/nsql-sqllogictest/sqllogictest"),
    r"^.*/*.slt",
);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TypeWrapper(LogicalType);

impl ColumnType for TypeWrapper {
    fn from_char(value: char) -> Option<Self> {
        let ty = match value {
            'I' => LogicalType::Int,
            'B' => LogicalType::Bool,
            'D' => LogicalType::Decimal,
            'T' => LogicalType::Text,
            _ => return None,
        };
        Some(TypeWrapper(ty))
    }

    fn to_char(&self) -> char {
        match self.0 {
            LogicalType::Int => 'I',
            LogicalType::Bool => 'B',
            LogicalType::Decimal => 'D',
            LogicalType::Text => 'T',
            LogicalType::Null => todo!(),
            LogicalType::Tid => todo!(),
        }
    }
}

pub struct TestDb<S> {
    db: Nsql<S>,
    connections: BTreeMap<Option<String>, Connection<'_, S>>,
}

impl TestDb {
    pub fn new(db: Nsql) -> Self {
        Self { db, connections: Default::default() }
    }
}

#[async_trait]
impl AsyncDB for TestDb {
    type Error = nsql::Error;

    type ColumnType = TypeWrapper;

    #[tracing::instrument(skip(self))]
    async fn run_on(
        &mut self,
        connection_name: Option<&str>,
        sql: &str,
    ) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let conn = self
            .connections
            .entry(connection_name.map(Into::into))
            .or_insert_with(|| self.db.connect());

        let output = conn.query(sql).await?;
        Ok(DBOutput::Rows {
            types: output.types.into_iter().map(TypeWrapper).collect(),
            rows: output
                .tuples
                .iter()
                .map(|t| t.values().map(|v| v.to_string()).collect())
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
