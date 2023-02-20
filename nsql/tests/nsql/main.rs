#![feature(never_type)]

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use nsql::Nsql;
use nsql_pager::Pager;
use sqllogictest::{AsyncDB, ColumnType, DBOutput, Runner, TestError};
use walkdir::WalkDir;

#[test]
fn nsql_sqllogictest() -> nsql::Result<(), Vec<TestError>> {
    nsql_test::start(async {
        let mut errors = vec![];
        let db = TestDb(Nsql::mem());
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
        Ok(())
    })
}

pub struct TestDb<P>(Nsql<P>);

// FIXME this isn't actually safe of course, but I don't think `AsyncDb` current actually uses the fact
// that `AsyncDB: Send` so it's fine for now. i.e. the crate will still compile with the `Send` bound removed.
// It might be one day that we have to get a sync implementation of `SingleFilePager` anyway for other reasons
unsafe impl<P: Pager> Send for TestDb<P> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {}

impl ColumnType for Type {
    fn from_char(value: char) -> Option<Self> {
        todo!()
    }

    fn to_char(&self) -> char {
        todo!()
    }
}

#[async_trait]
impl<P: Pager> AsyncDB for TestDb<P> {
    type Error = nsql::Error;

    type ColumnType = Type;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.0.query(sql)?;
        todo!()
    }

    fn engine_name(&self) -> &str {
        "nsql"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
