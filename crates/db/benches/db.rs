use criterion::{criterion_group, criterion_main, Criterion};
use db::{backend::memory::MemoryDB, DBTransaction, DBWrite, DB};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("std::HashMap<Vec<u8>, Vec<u8>> insert", |b| {
        let mut db = std::collections::HashMap::new();
        let mut i = 0;
        b.iter(|| {
            db.insert(
                usize::to_be_bytes(i).to_vec(),
                usize::to_be_bytes(i).to_vec(),
            );
            i += 1;
        })
    });
    c.bench_function("MemoryDB insert", |b| {
        let db = MemoryDB::default();
        let mut i = 0;
        b.iter(|| {
            let mut writer = db.start_transaction().unwrap();
            writer
                .set(usize::to_be_bytes(i), usize::to_be_bytes(i))
                .unwrap();
            writer.commit().unwrap();
            i += 1;
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
