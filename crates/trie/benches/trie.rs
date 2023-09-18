use criterion::{criterion_group, criterion_main, Criterion};
use db::backend::{memory::MemoryDB, rocks::RocksDB};
use trie::{
    backend::{db::TrieDBBackend, memory::TrieMemoryBackend},
    Op, Trie, TrieKey, TrieRef,
};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("[rocksdb backend] trie apply 100 ops ordered", |b| {
        let mut trie = Trie::new(
            TrieDBBackend::<_, u64, u64>::init(
                RocksDB::open_or_create_database("./ROCKS").unwrap(),
            )
            .unwrap(),
        );
        let mut i = 0;
        b.iter_batched(
            || {
                (0..100)
                    .map(|_| {
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef::from(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef::from(i as u128),
                            child_content: i,
                        }
                    })
                    .collect::<Vec<_>>()
            },
            |ops| {
                let mut writer = trie.write().unwrap();
                writer.apply(ops).unwrap();
                writer.commit().unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
    c.bench_function("[db backend] trie apply 100 ops ordered", |b| {
        let mut trie = Trie::new(TrieDBBackend::<_, u64, u64>::init(MemoryDB::default()).unwrap());
        let mut i = 0;
        b.iter_batched(
            || {
                (0..100)
                    .map(|_| {
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef::from(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef::from(i as u128),
                            child_content: i,
                        }
                    })
                    .collect::<Vec<_>>()
            },
            |ops| {
                let mut writer = trie.write().unwrap();
                writer.apply(ops).unwrap();
                writer.commit().unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
    c.bench_function(
        "[db backend] trie undo 100 ops and apply 1 op and redo 100 ops",
        |b| {
            let mut trie =
                Trie::new(TrieDBBackend::<_, u64, u64>::init(MemoryDB::default()).unwrap());

            let mut writer = trie.write().unwrap();
            writer
                .apply(
                    (0..100)
                        .map(|i| {
                            let i = i + u64::MAX - 100;
                            Op {
                                marker: i,
                                parent_ref: TrieRef::from(0),
                                child_key: TrieKey(format!("{}", i)),
                                child_ref: TrieRef::from(i as u128),
                                child_content: 0,
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            writer.commit().unwrap();
            let mut i = 0;
            b.iter_batched(
                || {
                    vec![{
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef::from(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef::from(i as u128),
                            child_content: i,
                        }
                    }]
                },
                |ops| {
                    let mut writer = trie.write().unwrap();
                    writer.apply(ops).unwrap();
                    writer.commit().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );
    c.bench_function("[memory backend] trie apply 100 ops ordered", |b| {
        let mut trie = Trie::new(TrieMemoryBackend::<u64, u64>::default());
        let mut i = 0;
        b.iter_batched(
            || {
                (0..100)
                    .map(|_| {
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef::from(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef::from(i as u128),
                            child_content: i,
                        }
                    })
                    .collect::<Vec<_>>()
            },
            |ops| {
                let mut writer = trie.write().unwrap();
                writer.apply(ops).unwrap();
                writer.commit().unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
    c.bench_function(
        "[memory backend] trie undo 100 ops and apply 1 op and redo 100 ops",
        |b| {
            let mut trie = Trie::new(TrieMemoryBackend::<u64, u64>::default());

            let mut writer = trie.write().unwrap();
            writer
                .apply(
                    (0..100)
                        .map(|i| {
                            let i = i + u64::MAX - 100;
                            Op {
                                marker: i,
                                parent_ref: TrieRef::from(0),
                                child_key: TrieKey(format!("{}", i)),
                                child_ref: TrieRef::from(i as u128),
                                child_content: 0,
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            writer.commit().unwrap();
            let mut i = 0;
            b.iter_batched(
                || {
                    vec![{
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef::from(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef::from(i as u128),
                            child_content: i,
                        }
                    }]
                },
                |ops| {
                    let mut writer = trie.write().unwrap();
                    writer.apply(ops).unwrap();
                    writer.commit().unwrap();
                },
                criterion::BatchSize::SmallInput,
            )
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
