use std::fmt;

use nsql_serde::{DeserializeSync, SerializeSync};
use test_strategy::proptest;

use super::{FileHeader, PagerHeader};
use crate::PAGE_SIZE;

#[proptest]
fn test_serde_db_header(expected: PagerHeader) {
    test_serde(expected);
}

#[proptest]
fn test_serde_file_header(expected: FileHeader) {
    test_serde(expected);
}

fn test_serde<T: fmt::Debug + Eq + SerializeSync + DeserializeSync>(expected: T) {
    let mut buf = [0; PAGE_SIZE];
    expected.serialize_sync(&mut &mut buf[..]);
    let deserialized = T::deserialize_sync(&mut &buf[..]);
    assert_eq!(expected, deserialized);
}
