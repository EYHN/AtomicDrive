use bumpalo::Bump;
#[cfg(not(codspeed))]
pub use criterion::*;
#[cfg(codspeed)]
pub use codspeed_criterion_compat::*;
use db::{
    backend::{memory::MemoryDB, rocks::RocksDB},
    DBRead, DBTransaction, DBWrite, DB,
};

fn criterion_benchmark(c: &mut Criterion) {
    let mut rocks_db =
        RocksDB::open_or_create_database(test_results::save_dir!("rocksdb")).unwrap();
    {
        let mut group = c.benchmark_group("insert");
        group.bench_function("std::HashMap<Vec<u8>, Vec<u8>>", |b| {
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
        group.bench_function("std::BTreeMap<Vec<u8>, Vec<u8>>", |b| {
            let mut db = std::collections::BTreeMap::new();
            let mut i = 0;
            b.iter(|| {
                db.insert(
                    usize::to_be_bytes(i).to_vec(),
                    usize::to_be_bytes(i).to_vec(),
                );
                i += 1;
            })
        });
        group.bench_function("MemoryDB", |b| {
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
        group.bench_function("MemoryDB with bump alloc", |b| {
            let bump = Bump::new();

            let db = MemoryDB::new_in(&bump);
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
        group.bench_function("RocksDB", |b| {
            rocks_db.clear().unwrap();
            let mut i = 0;
            b.iter(|| {
                let mut writer = rocks_db.start_transaction().unwrap();
                writer
                    .set(usize::to_be_bytes(i), usize::to_be_bytes(i))
                    .unwrap();
                writer.commit().unwrap();
                i += 1;
            })
        });
    }

    {
        let mut group = c.benchmark_group("get");
        group.bench_function("std::HashMap<Vec<u8>, Vec<u8>>", |b| {
            let mut db = std::collections::HashMap::new();
            for i in 0..10000 {
                db.insert(
                    usize::to_be_bytes(i).to_vec(),
                    usize::to_be_bytes(i).to_vec(),
                );
            }
            let target = usize::to_be_bytes(5000).to_vec();
            b.iter(|| {
                db.get(black_box(&target));
            })
        });
        group.bench_function("std::BTreeMap<Vec<u8>, Vec<u8>>", |b| {
            let mut db = std::collections::BTreeMap::new();
            for i in 0..10000 {
                db.insert(
                    usize::to_be_bytes(i).to_vec(),
                    usize::to_be_bytes(i).to_vec(),
                );
            }
            let target = usize::to_be_bytes(5000).to_vec();
            b.iter(|| {
                db.get(black_box(&target));
            })
        });
        group.bench_function("MemoryDB", |b| {
            let db = MemoryDB::default();
            let mut writer = db.start_transaction().unwrap();
            for i in 0..10000 {
                writer
                    .set(usize::to_be_bytes(i), usize::to_be_bytes(i))
                    .unwrap();
            }
            writer.commit().unwrap();
            let target = usize::to_be_bytes(5000).to_vec();
            b.iter(|| {
                db.get(black_box(&target)).unwrap();
            })
        });
        group.bench_function("RocksDB", |b| {
            rocks_db.clear().unwrap();
            let mut writer = rocks_db.start_transaction().unwrap();
            for i in 0..10000 {
                writer
                    .set(usize::to_be_bytes(i), usize::to_be_bytes(i))
                    .unwrap();
            }
            writer.commit().unwrap();
            let target = usize::to_be_bytes(5000).to_vec();
            b.iter(|| {
                rocks_db.get(black_box(&target)).unwrap();
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
