#![feature(never_type)]

use std::time::Duration;

use async_trait::async_trait;
use nsql::Nsql;
use sqllogictest::{AsyncDB, ColumnType, DBOutput};

pub struct TestDb<P>(Nsql<P>);

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
impl<P: Send + Sync> AsyncDB for TestDb<P> {
    type Error = nsql::Error;

    type ColumnType = Type;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        todo!()
    }

    fn engine_name(&self) -> &str {
        "nsql"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}

#[test]
fn test_nsql() {}
