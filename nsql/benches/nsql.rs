#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]

use std::iter;

use criterion::{BenchmarkId, Criterion, Throughput};
use criterion_macro::criterion;
use nsql::{LmdbStorageEngine, Nsql};

#[criterion]
pub fn bench_insert(c: &mut Criterion) {
    run(
        c,
        "insert",
        [10, 100, 1000, 10000, 100000, 1000000],
        Throughput::Elements,
        |_size| vec!["CREATE TABLE t (id int PRIMARY KEY)".into()],
        |size| format!("INSERT INTO t SELECT * FROM UNNEST(range(1, {size}))",),
    );
}

#[criterion]
pub fn bench_nested_loop_cross_join(c: &mut Criterion) {
    run(
        c,
        "nested loop cross join",
        [10, 100, 1000, 5000],
        |size| Throughput::Elements(size * size),
        |size| {
            vec![
                "CREATE TABLE t (id int PRIMARY KEY)".into(),
                format!("INSERT INTO t SELECT * FROM UNNEST(range(1, {size}))"),
            ]
        },
        |_| "SELECT * FROM t JOIN t".to_string(),
    );
}

/// Benchmark the overhead of binding and planning a trivial query
#[criterion]
pub fn bench_trivial_query_overhead(c: &mut Criterion) {
    run(
        c,
        "trivial query overhead",
        [100, 1000, 5000, 10000],
        Throughput::Elements,
        |_size| {
            vec!["CREATE TABLE t (id int PRIMARY KEY)".into(), "INSERT INTO t VALUES (1)".into()]
        },
        |size| iter::once("SELECT * FROM t;".repeat(size)).collect(),
    );
}

fn run<const N: usize>(
    c: &mut Criterion,
    name: &str,
    sizes: [usize; N],
    throughput: impl Fn(u64) -> Throughput,
    setup: impl Fn(usize) -> Vec<String>,
    test_sql: impl Fn(usize) -> String,
) {
    let mut group = c.benchmark_group(name);

    for size in sizes {
        group.throughput(throughput(size as u64));
        group.bench_with_input(BenchmarkId::new(name, size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let db_path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
                    let nsql = Nsql::<LmdbStorageEngine>::create(db_path).unwrap();
                    let conn = nsql.connect();

                    for setup in setup(size) {
                        conn.query(&setup).unwrap();
                    }

                    drop(conn);

                    (nsql, test_sql(size))
                },
                |(nsql, sql)| {
                    let conn = nsql.connect();
                    conn.query(&sql).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}
