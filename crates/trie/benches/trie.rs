use criterion::{criterion_group, criterion_main, Criterion};
use trie::{backend::memory::TrieMemoryBackend, Op, Trie, TrieKey, TrieRef};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("trie apply 100 ops ordered", |b| {
        let mut trie = Trie::new(TrieMemoryBackend::<u64, u64>::default());
        let mut i = 0;
        b.iter_batched(
            || {
                (0..100)
                    .map(|_| {
                        i += 1;
                        Op {
                            marker: i,
                            parent_ref: TrieRef(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef(i.into()),
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
    c.bench_function("trie undo 100 ops and apply 1 op and redo 100 ops", |b| {
        let mut trie = Trie::new(TrieMemoryBackend::<u64, u64>::default());

        let mut writer = trie.write().unwrap();
        writer
            .apply(
                (0..100)
                    .map(|i| {
                        let i = i + u64::MAX - 100;
                        Op {
                            marker: i,
                            parent_ref: TrieRef(0),
                            child_key: TrieKey(format!("{}", i)),
                            child_ref: TrieRef(i.into()),
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
                        parent_ref: TrieRef(0),
                        child_key: TrieKey(format!("{}", i)),
                        child_ref: TrieRef(i.into()),
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
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
