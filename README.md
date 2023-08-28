# nsql

Very very WIP in-process toy SQL database written in Rust in a similar vein to SQLite.

### Goals

The next goal is to pass sqlites [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) test suite.
The next feature to be implemented is correlated subqueries.

### Storage Engines

The storage layer is backed by a key-value store, LMDB and redb are supported.
Other key value can be implemented via the `StorageEngine` trait provided
that the storage engine's `WriteTransaction` doesn't require a mutable reference to use.
