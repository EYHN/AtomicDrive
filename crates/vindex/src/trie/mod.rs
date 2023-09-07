use std::{
    cmp::Ordering,
    collections::{hash_map::Entry as HashMapEntry, HashMap, HashSet, LinkedList, VecDeque},
    fmt::Display,
};

use std::fmt::Debug;
use thiserror::Error;
use utils::tree_stringify;
use uuid::Uuid;

use std::hash::Hash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Operation may break the tree, or the tree is already broken. {0}")]
    TreeBroken(String),
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait TrieContent: Clone + Hash + Default {}
impl<A: Clone + Hash + Default> TrieContent for A {}

pub trait TrieMarker: PartialOrd + Clone + Hash {}
impl<A: PartialOrd + Clone + Hash> TrieMarker for A {}

const ROOT: TrieId = TrieId(0);
const CONFLICT: TrieId = TrieId(1);

/// Tree node id
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TrieId(u64);

impl Display for TrieId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl TrieId {
    pub fn inc(&self) -> Self {
        TrieId(self.0 + 1)
    }
}

/// The key of the tree
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieKey(String);

impl Display for TrieKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// The reference of the node, which is used to determine the node of the operation during the distributed operation
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct TrieRef(u128);

impl TrieRef {
    pub fn new() -> Self {
        TrieRef(Uuid::new_v4().to_u128_le())
    }
}

impl Display for TrieRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieHash([u8; 32]);

impl Display for TrieHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            &self
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<String>>()
                .join("")[0..7],
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TrieNode<C> {
    parent: TrieId,
    key: TrieKey,
    hash: TrieHash,
    children: HashMap<TrieKey, TrieId>,
    content: C,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op<M: TrieMarker, C: TrieContent> {
    marker: M,
    parent_ref: TrieRef,
    child_key: TrieKey,
    child_ref: TrieRef,
    child_content: C,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogOp<M: TrieMarker, C: TrieContent> {
    op: Op<M, C>,
    undos: Vec<Undo<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConflictMode {
    KeepNew,
    KeepBefore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Do<C: TrieContent> {
    Ref(TrieRef, Option<TrieId>),
    Move {
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Undo<C: TrieContent> {
    Ref(TrieRef, Option<TrieId>),
    Move {
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct Trie<M: TrieMarker, C: TrieContent> {
    /// id -> node
    tree: HashMap<TrieId, TrieNode<C>>,
    /// ref <-> id index
    ref_id_index: (HashMap<TrieRef, TrieId>, HashMap<TrieId, HashSet<TrieRef>>),

    auto_increment_id: TrieId,

    log: LinkedList<LogOp<M, C>>,
}

impl<M: TrieMarker, C: TrieContent + Display> Display for Trie<M, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        self.itemization(TrieId(0), "", &mut items);

        f.write_str(&tree_stringify(
            items.iter().map(|(path, _id, node)| {
                (
                    path.as_ref(),
                    if node.content.to_string().is_empty() {
                        "".to_string()
                    } else {
                        format!("[{}]", node.content)
                    },
                )
            }),
            "/",
        ))
    }
}

impl<M: TrieMarker, C: TrieContent + Display> Debug for Trie<M, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        self.itemization(TrieId(0), "", &mut items);

        f.write_str(&tree_stringify(
            items.iter().map(|(path, id, node)| {
                (
                    path.as_ref(),
                    format!("[{}] #{} {}", node.content, id, node.hash),
                )
            }),
            "/",
        ))
    }
}

impl<M: TrieMarker, C: TrieContent> Default for Trie<M, C> {
    fn default() -> Self {
        Trie {
            tree: HashMap::from([
                (
                    ROOT,
                    TrieNode {
                        parent: ROOT,
                        key: TrieKey(Default::default()),
                        hash: Default::default(),
                        children: Default::default(),
                        content: Default::default(),
                    },
                ),
                (
                    CONFLICT,
                    TrieNode {
                        parent: CONFLICT,
                        key: TrieKey(Default::default()),
                        hash: Default::default(),
                        children: Default::default(),
                        content: Default::default(),
                    },
                ),
            ]),
            ref_id_index: (
                HashMap::from([(TrieRef(0), ROOT)]),
                HashMap::from([(ROOT, HashSet::from([TrieRef(0)]))]),
            ),
            auto_increment_id: TrieId(10),
            log: LinkedList::new(),
        }
    }
}

impl<M: TrieMarker, C: TrieContent> Trie<M, C> {
    fn get_id(&self, r: &TrieRef) -> Option<&TrieId> {
        self.ref_id_index.0.get(r)
    }

    fn get_refs(&self, id: &TrieId) -> Option<&HashSet<TrieRef>> {
        self.ref_id_index.1.get(id)
    }

    fn get(&self, id: &TrieId) -> Option<&TrieNode<C>> {
        self.tree.get(id)
    }

    fn get_ensure(&self, id: &TrieId) -> Result<&TrieNode<C>> {
        self.tree
            .get(id)
            .ok_or(Error::TreeBroken(format!("Trie id {id} not found")))
    }

    /// returns true if ancestor_id is an ancestor of child_id in tree.
    ///
    /// ```text
    /// parent | child
    /// --------------
    /// 1        2
    /// 1        3
    /// 3        5
    /// 2        6
    /// 6        8
    ///
    ///                  1
    ///               2     3
    ///             6         5
    ///           8
    ///
    /// is 2 ancestor of 8?  yes.
    /// is 2 ancestor of 5?   no.
    /// ```
    fn is_ancestor(&self, child_id: &TrieId, ancestor_id: &TrieId) -> bool {
        let mut target_id = child_id;
        while let Some(node) = self.get(target_id) {
            if &node.parent == ancestor_id {
                return true;
            }
            target_id = &node.parent;
            if target_id == &TrieId(0) {
                break;
            }
        }
        false
    }

    pub fn write(&mut self) -> TrieUpdater<'_, M, C> {
        TrieUpdater { target: self }
    }

    fn itemization<'a>(
        &'a self,
        root: TrieId,
        prefix: &str,
        base: &mut Vec<(String, TrieId, &'a TrieNode<C>)>,
    ) {
        if let Some(node) = self.get(&root) {
            let path = format!("{}/{}", prefix, node.key);
            base.push((path.clone(), root, node));

            for (_, id) in node.children.iter() {
                self.itemization(*id, &path, base)
            }
        }
    }
}

struct TrieUpdater<'a, M: TrieMarker, C: TrieContent> {
    target: &'a mut Trie<M, C>,
}

impl<M: TrieMarker, C: TrieContent> TrieUpdater<'_, M, C> {
    fn do_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Option<TrieId> {
        let old_id = if let Some(id) = self.target.ref_id_index.0.get(&r) {
            if let Some(refs) = self.target.ref_id_index.1.get_mut(id) {
                if refs.remove(&r) && refs.is_empty() {
                    self.target.ref_id_index.1.remove(id);
                }
            }
            Some(id.to_owned())
        } else {
            None
        };
        if let Some(id) = id {
            self.target.ref_id_index.0.insert(r.to_owned(), id);
            match self.target.ref_id_index.1.entry(id) {
                HashMapEntry::Occupied(mut entry) => {
                    entry.get_mut().insert(r);
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(HashSet::from([r]));
                }
            }
        }
        old_id
    }

    fn create_id(&mut self) -> TrieId {
        self.target.auto_increment_id = self.target.auto_increment_id.inc();
        self.target.auto_increment_id
    }

    fn move_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        let node = self.target.tree.remove(&id);

        if let Some(node) = &node {
            if let Some(parent) = self.target.tree.get_mut(&node.parent) {
                if parent.children.remove(&node.key).is_none() {
                    return Err(Error::TreeBroken("bad state".to_string()));
                }
            }
        }

        if let Some(to) = to {
            if let Some(n) = self.target.tree.get_mut(&to.0) {
                match n.children.entry(to.1.to_owned()) {
                    HashMapEntry::Occupied(_) => {
                        return Err(Error::TreeBroken(format!("key {:?} occupied", to.1)));
                    }
                    HashMapEntry::Vacant(entry) => {
                        entry.insert(id);
                    }
                }
            } else {
                return Err(Error::TreeBroken(format!("node id {:?} not found", to.0)));
            }

            self.target.tree.insert(
                id,
                TrieNode {
                    parent: to.0,
                    key: to.1,
                    hash: Default::default(),
                    children: node
                        .as_ref()
                        .map(|n| n.children.clone())
                        .unwrap_or_default(),
                    content: to.2,
                },
            );
        }

        Ok(node.map(|n| (n.parent, n.key, n.content)))
    }

    fn do_op(&mut self, op: Op<M, C>) -> Result<LogOp<M, C>> {
        let mut dos: Vec<Do<C>> = Default::default();
        let child_id = if let Some(child_id) = self.target.get_id(&op.child_ref) {
            child_id.to_owned()
        } else {
            let new_id = self.create_id();
            dos.push(Do::Ref(op.child_ref.to_owned(), Some(new_id)));
            new_id
        };
        let parent_id = if let Some(parent_id) = self.target.get_id(&op.parent_ref) {
            parent_id.to_owned()
        } else {
            return Err(Error::TreeBroken(format!(
                "parent ref {:?} not found",
                &op.parent_ref
            )));
        };

        let parent_node = self.target.get_ensure(&parent_id)?;

        // ensures no cycles are introduced.
        'c: {
            if child_id != parent_id && !self.target.is_ancestor(&parent_id, &child_id) {
                let old_node = self.target.get(&child_id);

                if let Some(conflict_node_id) = parent_node.children.get(&op.child_key).cloned() {
                    if conflict_node_id != child_id {
                        let conflict_node = self.target.get_ensure(&conflict_node_id)?.to_owned();
                        let conflict_is_empty = conflict_node.children.is_empty();
                        let new_is_empty = old_node.map(|n| n.children.is_empty()).unwrap_or(true);
                        if !conflict_is_empty && new_is_empty {
                            // new is empty, keep before
                            dos.push(Do::Ref(op.child_ref.to_owned(), Some(conflict_node_id)));

                            dos.push(Do::Move {
                                id: child_id,
                                to: Some((
                                    CONFLICT,
                                    TrieKey(child_id.to_string()),
                                    op.child_content.to_owned(),
                                )),
                            });
                            break 'c;
                        } else {
                            // keep new
                            let refs = self
                                .target
                                .get_refs(&conflict_node_id)
                                .map(|r| r.iter().cloned().collect::<Vec<_>>())
                                .unwrap_or_default();

                            for r in refs {
                                dos.push(Do::Ref(r, Some(child_id)));
                            }

                            dos.push(Do::Move {
                                id: conflict_node_id,
                                to: Some((
                                    CONFLICT,
                                    TrieKey(conflict_node_id.to_string()),
                                    conflict_node.content,
                                )),
                            });

                            dos.push(Do::Move {
                                id: child_id,
                                to: Some((
                                    parent_id,
                                    TrieKey(op.child_key.to_string()),
                                    op.child_content.to_owned(),
                                )),
                            });
                            break 'c;
                        }
                    }
                };

                dos.push(Do::Move {
                    id: child_id,
                    to: Some((
                        parent_id,
                        TrieKey(op.child_key.to_string()),
                        op.child_content.to_owned(),
                    )),
                });
            }
        }

        let mut undos: VecDeque<_> = VecDeque::new();

        for d in dos {
            undos.push_front(self.exec_do(d)?)
        }

        Ok(LogOp {
            op,
            undos: undos.into_iter().collect(),
        })
    }

    fn exec_do(&mut self, d: Do<C>) -> Result<Undo<C>> {
        Ok(match d {
            Do::Ref(r, id) => {
                let old_id = self.do_ref(r.to_owned(), id);
                Undo::Ref(r, old_id)
            }
            Do::Move { id, to } => {
                let old = self.move_node(id, to)?;
                Undo::Move { id, to: old }
            }
        })
    }

    fn exec_undo(&mut self, d: Undo<C>) -> Result<()> {
        match d {
            Undo::Ref(r, id) => {
                self.do_ref(r, id);
            }
            Undo::Move { id, to } => {
                self.move_node(id, to)?;
            }
        };
        Ok(())
    }

    fn undo_op(&mut self, log: &LogOp<M, C>) -> Result<()> {
        for undo in log.undos.iter().cloned() {
            self.exec_undo(undo)?
        }

        Ok(())
    }

    fn redo_op(&mut self, log: &LogOp<M, C>) -> Result<LogOp<M, C>> {
        self.do_op(log.op.clone())
    }

    fn apply(&mut self, ops: Vec<Op<M, C>>) -> Result<&mut Self> {
        let mut redo_queue = VecDeque::new();
        if let Some(first_op) = ops.first() {
            loop {
                if let Some(last) = self.target.log.pop_back() {
                    match first_op.marker.partial_cmp(&last.op.marker) {
                        None | Some(Ordering::Equal) => {
                            panic!("op with timestamp equal to previous op ignored. (not applied).  Every op must have a unique timestamp.");
                        }
                        Some(Ordering::Less) => {
                            self.undo_op(&last)?;
                            redo_queue.push_front(last)
                        }
                        Some(Ordering::Greater) => {
                            self.target.log.push_back(last);
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        for op in ops {
            loop {
                if let Some(redo) = redo_queue.pop_front() {
                    match op.marker.partial_cmp(&redo.op.marker) {
                        None | Some(Ordering::Equal) => {
                            return Err(Error::InvalidOp(
                                "The marker of the operation has duplicates. Every op must have a unique timestamp.".to_string(),
                            ));
                        }
                        Some(Ordering::Less) => {
                            let log_op = self.do_op(op)?;
                            self.target.log.push_back(log_op);
                            redo_queue.push_front(redo);
                            break;
                        }
                        Some(Ordering::Greater) => {
                            let redo_log_op = self.redo_op(&redo)?;
                            self.target.log.push_back(redo_log_op);
                            break;
                        }
                    }
                } else {
                    let log_op = self.do_op(op)?;
                    self.target.log.push_back(log_op);
                    break;
                }
            }
        }

        for redo in redo_queue {
            self.redo_op(&redo)?;
            self.target.log.push_back(redo);
        }

        Ok(self)
    }

    pub fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crdts::{CmRDT, VClock};
    use utils::PathTools;

    use indoc::indoc;

    use super::{Op, Trie, TrieId, TrieKey, TrieRef};

    #[derive(Debug, Hash, Clone, PartialEq, Eq)]
    struct Marker {
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
    struct End {
        actor: u64,
        clock: VClock<u64>,
        time: u64,
        trie: Trie<Marker, String>,
    }

    impl End {
        fn new(a: u64) -> Self {
            End {
                actor: a.to_owned(),
                clock: Default::default(),
                time: 0,
                trie: Default::default(),
            }
        }

        fn clone_as(&self, a: u64) -> Self {
            let mut new = self.clone();
            new.actor = a;
            new
        }

        fn ops_after(&self, after: &VClock<u64>) -> Vec<Op<Marker, String>> {
            let mut result = VecDeque::new();
            for log in self.trie.log.iter().rev() {
                let log_dot = log.op.marker.clock.dot(log.op.marker.actor);
                if log_dot > after.dot(log_dot.actor) {
                    result.push_front(log.op.clone())
                }
            }

            result.into_iter().collect()
        }

        fn sync_with(&mut self, other: &mut Self) {
            let other_ops = other.ops_after(&self.clock);
            self.apply(other_ops);

            let self_ops = self.ops_after(&other.clock);
            other.apply(self_ops);
        }

        fn apply(&mut self, ops: Vec<Op<Marker, String>>) {
            for op in ops.iter() {
                self.clock
                    .apply(op.marker.clock.dot(op.marker.actor))
            }
            self.trie.write().apply(ops).unwrap().commit();
        }

        fn get_id(&self, path: &str) -> TrieId {
            let mut id = TrieId(0);
            if path != "/" {
                for part in path.split('/').skip(1) {
                    id = *self
                        .trie
                        .get(&id)
                        .unwrap()
                        .children
                        .get(&TrieKey(part.to_string()))
                        .unwrap()
                }
            }

            id
        }

        fn get_ref(&self, path: &str) -> TrieRef {
            let id = self.get_id(path);
            self.trie
                .get_refs(&id)
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .to_owned()
        }

        fn get_content(&self, path: &str) -> String {
            let id = self.get_id(path);
            self.trie.get(&id).unwrap().content.to_owned()
        }

        fn rename(&mut self, from: &str, to: &str) {
            let content = self.get_content(from);
            let from = self.get_ref(from);
            let filename = PathTools::basename(to).to_owned();
            let to = self.get_ref(PathTools::dirname(to));

            self.clock.apply(self.clock.inc(self.actor));

            self.apply(vec![Op {
                marker: Marker {
                    actor: self.actor,
                    clock: self.clock.clone(),
                    time: self.time,
                },
                parent_ref: to,
                child_key: TrieKey(filename),
                child_ref: from,
                child_content: content,
            }])
        }

        fn write(&mut self, to: &str, data: &str) {
            let filename = PathTools::basename(to).to_owned();
            let to = self.get_ref(PathTools::dirname(to));

            self.clock.apply(self.clock.inc(self.actor));

            self.apply(vec![Op {
                marker: Marker {
                    actor: self.actor,
                    clock: self.clock.clone(),
                    time: self.time,
                },
                parent_ref: to,
                child_key: TrieKey(filename),
                child_ref: TrieRef::new(),
                child_content: data.to_owned(),
            }])
        }

        fn mkdir(&mut self, to: &str) {
            self.write(to, "")
        }

        fn date(&mut self, c: u64) {
            self.time = c
        }
    }

    fn check(ends: &[&End], expect: &str) {
        for a in ends.iter() {
            assert!(ends
                .iter()
                .all(|b| a.trie.to_string() == b.trie.to_string()));
        }
        for e in ends {
            assert_eq!(e.trie.to_string(), expect);
        }
    }

    macro_rules! testing {
        (show { $e:ident }) => {
            println!("{}", $e.trie.to_string());
        };
        (check $( $x:ident )* { $e:expr }) => {
            check(&[$(
                &$x,
            )*], indoc! {$e})
        };
        (sync { $from:ident <=> $to:ident }) => {
            $from.sync_with(&mut $to);
        };
        (have { $($end:ident($endid:literal))* }) => {
            $(let mut $end = End::new($endid);)*
        };
        (clone { $from:ident => $to:ident($toid:literal) }) => {
            let mut $to = $from.clone_as($toid);
        };
        (on $end:tt { $($ac:tt $($arg:expr)* );*; }) => {
            $(
                $end.$ac($($arg,)*);
            )*
        };
        ($($($cmd:ident)* { $($tail:tt)* })+) => {
            {
                $(testing!( $($cmd )* {  $($tail)* } );)*
            }
        };
    }

    #[test]
    fn write_with_rename() {
        testing!(
            have { local(1) }
            on local {
                mkdir "/hello";
                write "/hello/file" "world";
            }
            clone { local => remote(2) }
            on remote {
                rename "/hello" "/dir";
            }
            on local {
                write "/hello/file" "helloworld";
            }
            sync { local <=> remote }
            check local remote {
                "
                └ dir/file [helloworld]
                "
            }
        );
    }

    #[test]
    fn clock_test() {
        testing!(
            have { local(1) remote(2) }
            on local {
                date 0;
                write "/file" "local";
            }
            sync { local <=> remote }
            on remote {
                date 999;
                write "/file" "remote";
            }
            sync { local <=> remote }
            on local {
                date 0;
                write "/file" "some";
            }
            sync { local <=> remote }
            check local remote {
                // date does not affect sync if there are no conflicts
                "
                └ file [some]
                "
            }
        );
    }

    #[test]
    fn file_conflict_test() {
        testing!(
            have { local(1) remote(2) }
            on local {
                write "/file" "local";
            }
            on remote {
                write "/file" "remote";
            }
            sync { local <=> remote }
            check local remote {
                // remote id is larger, keep the remote
                "
                └ file [remote]
                "
            }
        );

        testing!(
            have { local(1) remote(2) }
            on local {
                date 2;
                write "/file" "local";
            }
            on remote {
                date 1;
                write "/file" "remote";
            }
            sync { local <=> remote }
            check local remote {
                // local date is larger, keep the local
                "
                └ file [local]
                "
            }
        );
    }

    #[test]
    fn folder_conflict_test() {
        testing!(
            have { local(1) remote(2) }
            on local {
                mkdir "/folder1";
                write "/folder1/foo" "bar";
            }
            on remote {
                mkdir "/folder1";
                write "/folder1/file" "abc";
            }
            sync { local <=> remote }
            on remote {
                mkdir "/folder1";
                write "/folder1/hello" "world";
            }
            sync { local <=> remote }
            check local remote {
                // no rename, we just merge the conflict folder
                "
                └ folder1
                 ├ file [abc]
                 ├ foo [bar]
                 └ hello [world]
                "
            }
        );

        testing!(
            have { local(1) }
            on local {
                mkdir "/folder1";
                write "/folder1/foo" "bar";
            }
            clone { local => remote(2) }
            on remote {
                mkdir "/folder2";
                write "/folder2/hello" "world";
                rename "/folder2" "/folder3";
            }
            on local {
                rename "/folder1" "/folder3";
            }
            sync { local <=> remote }
            check local remote {
                // both version of folder3 has content, we can't merge them
                // remote id is larger, keep the remote
                "
                └ folder3/hello [world]
                "
            }
        );

        testing!(
            have { local(1) }
            on local {
                mkdir "/folder1";
                write "/folder1/foo" "bar";
            }
            clone { local => remote(2) }
            on remote {
                mkdir "/folder2";
                rename "/folder2" "/folder3";
                write "/folder3/hello" "world";
            }
            on local {
                rename "/folder1" "/folder3";
            }
            sync { local <=> remote }
            check local remote {
                // local version folder3 has content, keep the local
                // and writes after the rename on the remote will apply to new folder3
                // its looks like merge
                "
                └ folder3
                 ├ foo [bar]
                 └ hello [world]
                "
            }
        );
    }
}
