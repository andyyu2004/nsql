# nsql

Very very WIP in-process toy SQL database written in Rust in a similar vein to SQLite.

### Goals

The next goal is to pass sqlites [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) test suite.
The next feature to be implemented is correlated subqueries.

### Getting started

A nsql-shell can be acquired with `cargo r -r -- /path/to/db`. It will be created if it doesn't exist.

### Basic architecture

`nsql` is the library crate which drives the helper crates.

`nsql-parse` is a tiny wrapper around the excellent [sqlparser-rs](github.com/sqlparser-rs/sqlparser-rs) library.

`nsql-ir` contains the intermediate representation definitions (think relational algebra).

`nsql-catalog` contains catalog definition and is used by the binder to resolve table references etc.
This is bootstrapped in a manner similar to postgres' system catalogs. E.g. nsql catalog metadata is stored in nsql tables.
Try `SELECT * FROM nsql_catalog.nsql_table`.

`nsql-bind` contains the binder which lowers the ast from parsing into a `LogicalPlan` defined by `nsql-ir`.
This includes typechecking and name resolution.

`nsql-opt` is effectively a noop layer at this point in time. It does some subquery flattening but not much else yet.
This takes as input a non-optimized logical plan and outputs an optimized logical plan.

`nsql-execution` takes a logical plan and (naively) transforms it into a physical plan.
This physical plan is converted into pipelines and then executed.
The executor is currently a (naive) push-based executor inspired by DuckDB's.

### Storage Engines

The storage layer is backed by a key-value store, LMDB and redb are supported.
Other key value can be implemented via the `StorageEngine` trait provided
that the storage engine's `WriteTransaction` doesn't require a mutable reference to use.
