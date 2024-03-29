use std::{borrow::Borrow, collections::VecDeque};

use crdts::{CmRDT, VClock};
use db::backend::memory::MemoryDB;
use utils::{Deserialize, PathTools, Serialize, Serializer};

use super::super::{Op, Trie, TrieKey, TrieRef, TrieStoreRead};

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Marker {
    actor: u64,
    clock: VClock<u64>,
    time: u64,
}

impl Serialize for Marker {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer = self.actor.serialize(serializer);
        serializer = self.clock.dots.serialize(serializer);
        self.time.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.actor.byte_size()? + self.clock.dots.byte_size()? + self.time.byte_size()?)
    }
}

impl Deserialize for Marker {
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8]), String> {
        let (actor, bytes) = <_>::deserialize(bytes)?;
        let (dots, bytes) = <_>::deserialize(bytes)?;
        let (time, bytes) = <_>::deserialize(bytes)?;

        Ok((
            Self {
                actor,
                clock: VClock { dots },
                time,
            },
            bytes,
        ))
    }
}

impl Ord for Marker {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.clock.partial_cmp(&other.clock) {
            Some(std::cmp::Ordering::Equal) | None => match self.time.cmp(&other.time) {
                std::cmp::Ordering::Equal => self.actor.cmp(&other.actor),
                ord => ord,
            },
            Some(ord) => ord,
        }
    }
}

impl PartialOrd for Marker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct End {
    actor: u64,
    clock: VClock<u64>,
    time: u64,
    trie: Trie<Marker, String, MemoryDB>,
}

impl End {
    pub fn new(a: u64) -> Self {
        End {
            actor: a,
            clock: Default::default(),
            time: 0,
            trie: Trie::init(MemoryDB::default()).unwrap(),
        }
    }

    pub fn clone_as(&self, a: u64) -> Self {
        let mut new = self.clone();
        new.actor = a;
        new
    }

    pub fn ops_after(&self, after: &VClock<u64>) -> Vec<Op<Marker, String>> {
        let mut result = VecDeque::new();
        for log in self.trie.iter_log().unwrap() {
            let log = log.unwrap();
            let log = log.borrow();
            let log_dot = log.op.marker.clock.dot(log.op.marker.actor);
            if log_dot > after.dot(log_dot.actor) {
                result.push_front(log.op.clone())
            }
        }

        result.into_iter().collect()
    }

    pub fn sync_with(&mut self, other: &mut Self) {
        let other_ops = other.ops_after(&self.clock);
        for op in other_ops.iter() {
            self.clock.apply(op.marker.clock.dot(op.marker.actor))
        }
        let mut writer = self.trie.write().unwrap();

        writer.apply(other_ops).unwrap();
        writer.commit().unwrap();

        let self_ops = self.ops_after(&other.clock);
        for op in self_ops.iter() {
            other.clock.apply(op.marker.clock.dot(op.marker.actor))
        }
        let mut writer = other.trie.write().unwrap();

        writer.apply(self_ops).unwrap();
        writer.commit().unwrap();
    }

    pub fn rename(&mut self, from: &str, to: &str) {
        let mut writer = self.trie.write().unwrap();
        let from = writer
            .get_refs_by_path(from)
            .unwrap()
            .unwrap()
            .first()
            .unwrap()
            .clone();
        let filename = PathTools::basename(to).to_owned();
        let to = writer
            .get_refs_by_path(PathTools::dirname(to))
            .unwrap()
            .unwrap()
            .first()
            .unwrap()
            .clone();

        self.clock.apply(self.clock.inc(self.actor));

        writer
            .apply(vec![Op {
                marker: Marker {
                    actor: self.actor,
                    clock: self.clock.clone(),
                    time: self.time,
                },
                parent_target: to.into(),
                child_key: TrieKey(filename),
                child_target: from.into(),
                child_content: None,
            }])
            .unwrap();
        writer.commit().unwrap();
    }

    pub fn write(&mut self, to: &str, data: &str) {
        let filename = PathTools::basename(to).to_owned();
        let mut writer = self.trie.write().unwrap();
        let to = writer
            .get_refs_by_path(PathTools::dirname(to))
            .unwrap()
            .unwrap()
            .first()
            .unwrap()
            .clone();

        self.clock.apply(self.clock.inc(self.actor));

        writer
            .apply(vec![Op {
                marker: Marker {
                    actor: self.actor,
                    clock: self.clock.clone(),
                    time: self.time,
                },
                parent_target: to.into(),
                child_key: TrieKey(filename),
                child_target: TrieRef::new().into(),
                child_content: Some(data.to_owned()),
            }])
            .unwrap();
        writer.commit().unwrap();
    }

    pub fn mkdir(&mut self, to: &str) {
        self.write(to, "")
    }

    pub fn date(&mut self, c: u64) {
        self.time = c
    }
}

pub fn check(ends: &[&End], expect: &str) {
    for a in ends.iter() {
        for b in ends.iter() {
            assert_eq!(a.trie.to_string(), b.trie.to_string());
        }
    }
    for e in ends {
        assert_eq!(e.trie.to_string(), expect);
    }
}

macro_rules! testing {
    (show { $e:ident }) => {
        println!("{}", $e.trie.to_string());
    };
    (exec { $($e:expr;)+ }) => {
      $($e;)*
    };
    (check $( $x:ident )* { $e:expr }) => {
        tools::check(&[$(
            &$x,
        )*], indoc::indoc! {$e})
    };
    (sync { $from:ident <=> $to:ident }) => {
        $from.sync_with(&mut $to);
    };
    (have { $($end:ident($end_id:literal))* }) => {
        $(let mut $end = tools::End::new($end_id);)*
    };
    (clone { $from:ident => $to:ident($to_id:literal) }) => {
        let mut $to = $from.clone_as($to_id);
    };
    (on $end:tt { $($ac:tt $($arg:expr)* );*; }) => {
        $(
            $end.$ac($($arg,)*);
        )*
    };
    ($($cmd1:ident)* { $($tail1:tt)* } $($($cmd:ident)* { $($tail:tt)* })+) => {
      testing!( $($cmd1 )* {  $($tail1)* } );
      $(testing!( $($cmd )* {  $($tail)* } );)*
    };
}
