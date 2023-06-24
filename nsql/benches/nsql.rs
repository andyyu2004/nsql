use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nsql::{LmdbStorageEngine, Nsql};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for size in [100, 1000, 10000, 100000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let db_path = nsql_test::tempfile::NamedTempFile::new().unwrap().into_temp_path();
                let nsql = Nsql::<LmdbStorageEngine>::create(db_path).unwrap();
                let (conn, state) = nsql.connect();
                conn.query(&state, "CREATE TABLE t (id int PRIMARY KEY)").unwrap();
                conn.query(
                    &state,
                    &format!("INSERT INTO t SELECT * FROM UNNEST(range(1, {}))", size),
                )
                .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
