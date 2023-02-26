use std::fmt;

use nsql_serde::{Deserialize, Serialize};
use test_strategy::proptest;

use super::{FileHeader, PagerHeader};
use crate::PAGE_SIZE;

#[proptest]
fn test_serde_db_header(expected: PagerHeader) {
    nsql_test::start(test_serde(expected))
}

#[proptest]
fn test_serde_file_header(expected: FileHeader) {
    nsql_test::start(test_serde(expected))
}

async fn test_serde<T: fmt::Debug + Eq + Serialize + Deserialize>(expected: T) {
    let mut buf = vec![];
    expected.serialize(&mut buf).await.unwrap();
    let deserialized = T::deserialize(&mut &buf[..]).await.unwrap();
    assert_eq!(expected, deserialized);
}
