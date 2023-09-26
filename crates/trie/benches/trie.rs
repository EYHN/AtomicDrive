#[cfg(not(codspeed))]
pub use criterion::*;
#[cfg(codspeed)]
pub use codspeed_criterion_compat::*;
use db::backend::memory::MemoryDB;
use trie::trie::{Op, Trie, TrieKey, TrieRef};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("[db backend] trie apply 100 ops ordered", |b| {
        let mut trie = Trie::init(MemoryDB::default()).unwrap();
        let mut i = 0u64;
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
                            child_content: Some(i),
                        }
                    })
                    .collect::<Vec<_>>()
            },
            |ops| {
                let mut writer = trie.write().unwrap();
                writer.apply(ops).unwrap();
                writer.commit().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    c.bench_function(
        "[db backend] trie undo 100 ops and apply 1 op and redo 100 ops",
        |b| {
            let mut trie = Trie::init(MemoryDB::default()).unwrap();

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
                                child_content: Some(0),
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
                            child_content: Some(i),
                        }
                    }]
                },
                |ops| {
                    let mut writer = trie.write().unwrap();
                    writer.apply(ops).unwrap();
                    writer.commit().unwrap();
                },
                BatchSize::SmallInput,
            )
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
