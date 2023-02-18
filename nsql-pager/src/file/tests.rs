use std::fmt;

use test_strategy::proptest;

use super::{FileHeader, PagerHeader};
use crate::file::{Deserialize, Serialize};
use crate::PAGE_SIZE;

#[proptest]
fn test_serde_db_header(expected: PagerHeader) {
    test_serde(expected);
}

#[proptest]
fn test_serde_file_header(expected: FileHeader) {
    test_serde(expected);
}

fn test_serde<T: fmt::Debug + Eq + Serialize + Deserialize>(expected: T) {
    let mut buf = [0; PAGE_SIZE];
    expected.serialize(&mut buf);
    let deserialized = T::deserialize(&buf);
    assert_eq!(expected, deserialized);
}
