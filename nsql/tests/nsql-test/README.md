## nsql-test

This test suite contains tests that don't fit into the `sqllogictest` framework.
It should be preferred to use the `.slt` test if possible. Tests that don't work well are for example snapshots of `EXPLAIN` or `EXPLAIN ANALYZE`
where we don't want to rely on the exact output. This crate has auto updating features by using [expect-test](https://github.com/rust-analyzer/expect-test).
