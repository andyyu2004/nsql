use std::future::Future;

use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use nsql_btree::BTree;

criterion_group!(benches, bench_insertions);
criterion_main!(benches);

struct Runtime(tokio_uring::Runtime);

impl Default for Runtime {
    fn default() -> Self {
        Self(tokio_uring::Runtime::new(&tokio_uring::builder()).unwrap())
    }
}

impl AsyncExecutor for Runtime {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.0.block_on(future)
    }
}

fn bench_insertions(c: &mut Criterion) {
    c.bench_function("insertions", |b| b.to_async(Runtime::default()).iter(insertions));
}

async fn insertions() {
    let pool = nsql_test::mk_mem_buffer_pool!();
    let btree = BTree::<u32, u64>::init(pool).await.unwrap();
    for i in 0..1000 {
        btree.insert(i, i as u64).await.unwrap();
    }
}
