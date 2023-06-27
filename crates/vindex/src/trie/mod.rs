use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    collections::{hash_map::Entry as HashMapEntry, HashMap, HashSet, LinkedList, VecDeque},
    fmt::Display,
};

use crdts::{Actor, VClock};
use std::fmt::Debug;
use thiserror::Error;
use utils::tree_stringify;

use std::hash::Hash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Operation may break the tree, or the tree is already broken. message: `{0}`")]
    TreeBroken(String, Backtrace),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait TrieContent: Clone + Hash + Default {}
impl<A: Clone + Hash + Default> TrieContent for A {}

pub trait TrieClock: PartialOrd + Clone + Hash {}
impl<A: PartialOrd + Clone + Hash> TrieClock for A {}

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
pub struct Op<V: TrieClock, A: Actor, C: TrieContent> {
    clock: V,
    actor: A,
    parent_ref: TrieRef,
    child_key: TrieKey,
    child_ref: TrieRef,
    child_content: C,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogOp<V: TrieClock, A: Actor, C: TrieContent> {
    op: Op<V, A, C>,
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

#[derive(Debug)]
struct Trie<V: TrieClock, A: Actor, C: TrieContent> {
    /// id -> node
    tree: HashMap<TrieId, TrieNode<C>>,
    /// ref <-> id index
    ref_id_index: (HashMap<TrieRef, TrieId>, HashMap<TrieId, HashSet<TrieRef>>),

    auto_increment_id: TrieId,

    log: LinkedList<LogOp<V, A, C>>,
}

impl<V: TrieClock, A: Actor, C: TrieContent + Display> Display for Trie<V, A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        self.itemization(TrieId(0), "", &mut items);

        f.write_str(&tree_stringify(
            items.iter().map(|(path, id, node)| {
                (
                    path.as_ref(),
                    format!("[{}] #{} {}", node.content.to_string(), id, node.hash),
                )
            }),
            "/",
        ))
    }
}

impl<V: TrieClock, A: Actor, C: TrieContent> Default for Trie<V, A, C> {
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

impl<V: TrieClock, A: Actor, C: TrieContent> Trie<V, A, C> {
    fn ref_to_id(&self, r: &TrieRef) -> Option<&TrieId> {
        self.ref_id_index.0.get(r)
    }

    fn get(&self, id: &TrieId) -> Option<&TrieNode<C>> {
        self.tree.get(id)
    }

    fn get_ensure(&self, id: &TrieId) -> Result<&TrieNode<C>> {
        self.tree.get(id).ok_or(Error::TreeBroken(
            format!("Trie id {id} not found"),
            Backtrace::capture(),
        ))
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

    pub fn write(&mut self) -> TrieUpdater<'_, V, A, C> {
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

struct TrieUpdater<'a, V: TrieClock, A: Actor, C: TrieContent> {
    target: &'a mut Trie<V, A, C>,
}

impl<V: TrieClock, A: Actor, C: TrieContent> TrieUpdater<'_, V, A, C> {
    fn do_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Option<TrieId> {
        let old_id = if let Some(id) = self.target.ref_id_index.0.get(&r) {
            if let Some(refs) = self.target.ref_id_index.1.get_mut(&id) {
                if refs.remove(&r) {
                    if refs.is_empty() {
                        self.target.ref_id_index.1.remove(&id);
                    }
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
                    entry.get_mut().insert(r.to_owned());
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(HashSet::from([r]));
                }
            }
        }
        old_id
    }

    fn get_refs(&mut self, id: &TrieId) -> Option<&HashSet<TrieRef>> {
        self.target.ref_id_index.1.get(id)
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
                    return Err(Error::TreeBroken(
                        format!("bad state"),
                        std::backtrace::Backtrace::capture(),
                    ));
                }
            }
        }

        if let Some(to) = to {
            if let Some(n) = self.target.tree.get_mut(&to.0) {
                match n.children.entry(to.1.to_owned()) {
                    HashMapEntry::Occupied(_) => {
                        return Err(Error::TreeBroken(
                            format!("key {:?} occupied", to.1),
                            std::backtrace::Backtrace::capture(),
                        ));
                    }
                    HashMapEntry::Vacant(entry) => {
                        entry.insert(id);
                    }
                }
            } else {
                return Err(Error::TreeBroken(
                    format!("node id {:?} not found", to.0),
                    std::backtrace::Backtrace::capture(),
                ));
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

    fn do_op(&mut self, op: Op<V, A, C>) -> Result<LogOp<V, A, C>> {
        let mut dos: Vec<Do<C>> = Default::default();
        let child_id = if let Some(child_id) = self.target.ref_to_id(&op.child_ref) {
            child_id.to_owned()
        } else {
            let new_id = self.create_id();
            dos.push(Do::Ref(op.child_ref.to_owned(), Some(new_id)));
            new_id
        };
        let parent_id = if let Some(parent_id) = self.target.ref_to_id(&op.parent_ref) {
            parent_id.to_owned()
        } else {
            return Err(Error::TreeBroken(
                format!("parent ref {:?} not found", &op.parent_ref),
                std::backtrace::Backtrace::capture(),
            ));
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
                                    conflict_node.content.to_owned(),
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
        Ok(match d {
            Undo::Ref(r, id) => {
                self.do_ref(r, id);
            }
            Undo::Move { id, to } => {
                self.move_node(id, to)?;
            }
        })
    }

    fn undo_op(&mut self, log: &LogOp<V, A, C>) -> Result<()> {
        for undo in log.undos.iter().cloned() {
            self.exec_undo(undo)?
        }

        Ok(())
    }

    fn redo_op(&mut self, log: &LogOp<V, A, C>) -> Result<LogOp<V, A, C>> {
        self.do_op(log.op.clone())
    }

    fn apply(&mut self, ops: Vec<Op<V, A, C>>) -> Result<()> {
        let mut redo_queue = VecDeque::new();
        if let Some(first_op) = ops.first() {
            loop {
                if let Some(last) = self.target.log.pop_back() {
                    match first_op.clock.partial_cmp(&last.op.clock) {
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
                    match op.clock.partial_cmp(&redo.op.clock) {
                        None | Some(Ordering::Equal) => {
                            panic!("op with timestamp equal to previous op ignored. (not applied).  Every op must have a unique timestamp.");
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

        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::trie::TrieUpdater;

    use super::{Op, Trie, TrieKey, TrieRef};

    #[test]
    fn test() {
        let mut t = Trie::<u64, u64, String>::default();
        let mut w = t.write();
        fn mov(
            w: &mut TrieUpdater<'_, u64, u64, String>,
            clock: u64,
            p: u128,
            k: &str,
            c: u128,
            data: &str,
        ) {
            w.apply(vec![Op {
                clock,
                actor: 1,
                parent_ref: TrieRef(p),
                child_key: TrieKey(k.to_string()),
                child_ref: TrieRef(c),
                child_content: data.to_string(),
            }])
            .unwrap();
        }
        mov(&mut w, 1, 0, "abc", 1, "test folder");
        mov(&mut w, 3, 1, "hello", 2, "test file");
        mov(&mut w, 2, 1, "hello", 2, "test file2");
        mov(&mut w, 4, 0, "abc", 3, "test folder 2");
        mov(&mut w, 5, 3, "hello2", 4, "test file212");
        mov(&mut w, 6, 1, "hello3", 4, "test file212");
        mov(&mut w, 7, 1, "hello3", 4, "test file212");
        mov(&mut w, 8, 2, "world", 5, "test");
        mov(&mut w, 9, 0, "world", 2, "aaa");
        mov(&mut w, 10, 0, "abc", 2, "aaa");
        mov(&mut w, 11, 1, "cewafewa", 6, "aaaa");

        w.commit().unwrap();
        dbg!(&t);

        println!("{t}");
    }
}
