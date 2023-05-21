use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

use async_trait::async_trait;
use nsql::{Connection, Nsql};
use nsql_lmdb::LmdbStorageEngine;
use nsql_storage::schema::LogicalType;
use nsql_storage_engine::StorageEngine;
use sqllogictest::{ColumnType, DBOutput, Runner, DB};
use tracing_subscriber::EnvFilter;

fn nsql_sqllogictest(path: &Path) -> nsql::Result<(), Box<dyn Error>> {
    let filter =
        EnvFilter::try_from_env("NSQL_LOG").unwrap_or_else(|_| EnvFilter::new("nsql=DEBUG"));
    let _ = tracing_subscriber::fmt::fmt().with_env_filter(filter).try_init();
    let db_path = nsql_test::tempfile::NamedTempFile::new()?.into_temp_path();
    let db = TestDb::new(Nsql::<LmdbStorageEngine>::open(db_path).unwrap());
    let mut tester = Runner::new(db);
    tester.run_file(path)?;
    Ok(())
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

pub struct TestDb<S: StorageEngine> {
    db: Nsql<S>,
    connections: BTreeMap<Option<String>, Connection<'static, S>>,
}

impl<S: StorageEngine> TestDb<S> {
    pub fn new(db: Nsql<S>) -> Self {
        Self { db, connections: Default::default() }
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct ErrorWrapper(#[from] anyhow::Error);

#[async_trait]
impl<S: StorageEngine> DB for TestDb<S> {
    type Error = ErrorWrapper;

    type ColumnType = TypeWrapper;

    #[tracing::instrument(skip(self))]
    fn run_on(
        &mut self,
        connection_name: Option<&str>,
        sql: &str,
    ) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let conn = self
            .connections
            .entry(connection_name.map(Into::into))
            // Safety: We don't close the storage engine until the end of the test, so this
            // lifetime extension is ok.
            .or_insert_with(|| unsafe { std::mem::transmute(self.db.connect()) });

        let output = conn.query(sql)?;
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
}
