use std::{borrow::Borrow, collections::VecDeque};

use crdts::{CmRDT, VClock};
use utils::PathTools;

use crate::{
    backend::{memory::TrieMemoryBackend, TrieBackend},
    ROOT,
};

use crate::{Op, Trie, TrieId, TrieKey, TrieRef};

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Marker {
    actor: u64,
    clock: VClock<u64>,
    time: u64,
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
    trie: Trie<Marker, String, TrieMemoryBackend<Marker, String>>,
}

impl End {
    pub fn new(a: u64) -> Self {
        return End {
            actor: a,
            clock: Default::default(),
            time: 0,
            trie: Trie::new(TrieMemoryBackend::default()),
        };
    }

    pub fn clone_as(&self, a: u64) -> Self {
        let mut new = self.clone();
        new.actor = a;
        new
    }

    pub fn ops_after(&self, after: &VClock<u64>) -> Vec<Op<Marker, String>> {
        let mut result = VecDeque::new();
        for log in self.trie.backend.iter_log().unwrap() {
            let log = log.unwrap();
            let log_dot = log.op.marker.clock.dot(log.op.marker.actor.clone());
            if log_dot > after.dot(log_dot.actor.clone()) {
                result.push_front(log.op.clone())
            }
        }

        return result.into_iter().collect();
    }

    pub fn sync_with(&mut self, other: &mut Self) {
        let other_ops = other.ops_after(&self.clock);
        self.apply(other_ops);

        let self_ops = self.ops_after(&other.clock);
        other.apply(self_ops);
    }

    pub fn apply(&mut self, ops: Vec<Op<Marker, String>>) {
        for op in ops.iter() {
            self.clock
                .apply(op.marker.clock.dot(op.marker.actor.clone()))
        }
        let mut writer = self.trie.write().unwrap();

        writer.apply(ops).unwrap();
        writer.commit().unwrap();
    }

    pub fn get_id(&self, path: &str) -> TrieId {
        let mut id = ROOT;
        if path != "/" {
            for part in path.split('/').skip(1) {
                id = self
                    .trie
                    .get_child(id, TrieKey(part.to_string()))
                    .unwrap()
                    .unwrap()
            }
        }

        id
    }

    pub fn get_ref(&self, path: &str) -> TrieRef {
        let id = self.get_id(path);
        self.trie
            .get_refs(id)
            .unwrap()
            .unwrap()
            .next()
            .unwrap()
            .borrow()
            .to_owned()
    }

    pub fn get_content(&self, path: &str) -> String {
        let id = self.get_id(path);
        self.trie
            .get(id)
            .unwrap()
            .unwrap()
            .borrow()
            .content
            .to_owned()
    }

    pub fn rename(&mut self, from: &str, to: &str) {
        let content = self.get_content(from);
        let from = self.get_ref(from);
        let filename = PathTools::basename(to).to_owned();
        let to = self.get_ref(PathTools::dirname(to));

        self.clock.apply(self.clock.inc(self.actor.clone()));

        self.apply(vec![Op {
            marker: Marker {
                actor: self.actor.clone(),
                clock: self.clock.clone(),
                time: self.time,
            },
            parent_ref: to,
            child_key: TrieKey(filename),
            child_ref: from,
            child_content: content,
        }])
    }

    pub fn write(&mut self, to: &str, data: &str) {
        let filename = PathTools::basename(to).to_owned();
        let to = self.get_ref(PathTools::dirname(to));

        self.clock.apply(self.clock.inc(self.actor.clone()));

        self.apply(vec![Op {
            marker: Marker {
                actor: self.actor.clone(),
                clock: self.clock.clone(),
                time: self.time,
            },
            parent_ref: to,
            child_key: TrieKey(filename),
            child_ref: TrieRef::new(),
            child_content: data.to_owned(),
        }])
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
            assert_eq!(
                a.trie.get(ROOT).unwrap().unwrap().borrow().hash,
                b.trie.get(ROOT).unwrap().unwrap().borrow().hash
            );
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
        crate::tests::tools::check(&[$(
            &$x,
        )*], indoc::indoc! {$e})
    };
    (sync { $from:ident <=> $to:ident }) => {
        $from.sync_with(&mut $to);
    };
    (have { $($end:ident($end_id:literal))* }) => {
        $(let mut $end = crate::tests::tools::End::new($end_id);)*
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
