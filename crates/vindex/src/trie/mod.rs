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
pub struct Op<A: Actor, C: TrieContent> {
    clock: VClock<A>,
    actor: A,
    parent_ref: TrieRef,
    child_key: TrieKey,
    child_ref: TrieRef,
    child_content: C,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogOp<A: Actor, C: TrieContent> {
    op: Op<A, C>,
    undos: Vec<Undo<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConflictMode {
    KeepNew,
    KeepBefore,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Undo<C: TrieContent> {
    DeleteRef(TrieRef),
    RedirectRef(TrieRef, TrieId),
    Move {
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    },
}

// impl<A: Actor + Debug, C: TrieContent + Debug> Debug for LogOp<A, C> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let mut f = if let Some(from) = &self.from {
//             f.debug_struct(&format!(
//                 "LogOp(`{:?}` moved {} -> {}[{}] from {}[{}])",
//                 self.op.actor, self.op.child_ref, self.op.parent_ref, self.op.child_key, from.0, from.1
//             ))
//         } else {
//             f.debug_struct(&format!(
//                 "LogOp(`{:?}` moved {} -> {}[{}])",
//                 self.op.actor, self.child_id, self.parent_id, self.child_key
//             ))
//         };

//         f.field("content", &self.op.child_content);
//         if let Some(old) = &self.from {
//             f.field("old_content", &old.2);
//         }

//         f.finish()
//     }
// }

#[derive(Debug)]
struct Trie<A: Actor, C: TrieContent> {
    /// id -> node
    tree: HashMap<TrieId, TrieNode<C>>,
    /// ref <-> id index
    ref_id_index: (HashMap<TrieRef, TrieId>, HashMap<TrieId, HashSet<TrieRef>>),

    auto_increment_id: TrieId,

    log: LinkedList<LogOp<A, C>>,
}

impl<A: Actor, C: TrieContent + Display> Display for Trie<A, C> {
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

impl<A: Actor, C: TrieContent> Default for Trie<A, C> {
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

impl<A: Actor, C: TrieContent> Trie<A, C> {
    fn ref_to_id(&self, r: &TrieRef) -> Option<&TrieId> {
        self.ref_id_index.0.get(r)
    }

    fn get(&self, id: &TrieId) -> Option<&TrieNode<C>> {
        self.tree.get(id)
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

    pub fn write(&mut self) -> TrieUpdater<'_, A, C> {
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

struct TrieUpdater<'a, A: Actor, C: TrieContent> {
    target: &'a mut Trie<A, C>,
}

impl<A: Actor, C: TrieContent> TrieUpdater<'_, A, C> {
    fn create_ref(&mut self, r: TrieRef, id: TrieId) -> Result<()> {
        if self
            .target
            .ref_id_index
            .0
            .insert(r.to_owned(), self.target.auto_increment_id)
            .is_some()
        {
            return Err(Error::TreeBroken(
                format!("ref {r:?} already exsits"),
                std::backtrace::Backtrace::capture(),
            ));
        }
        match self.target.ref_id_index.1.entry(id) {
            HashMapEntry::Occupied(mut entry) => {
                if !entry.get_mut().insert(r.to_owned()) {
                    return Err(Error::TreeBroken(
                        format!("ref {r:?} already exsits"),
                        std::backtrace::Backtrace::capture(),
                    ));
                }
            }
            HashMapEntry::Vacant(entry) => {
                entry.insert(HashSet::from([r]));
            }
        }
        Ok(())
    }

    fn redirect_ref(&mut self, r: TrieRef, id: TrieId) -> Result<()> {
        self.remove_ref(&r)?;
        self.create_ref(r, id)?;
        Ok(())
    }

    fn get_refs(&mut self, id: &TrieId) -> Option<&HashSet<TrieRef>> {
        self.target.ref_id_index.1.get(id)
    }

    fn remove_ref(&mut self, r: &TrieRef) -> Result<()> {
        if let Some(id) = self.target.ref_id_index.0.remove(&r) {
            if let Some(refs) = self.target.ref_id_index.1.get_mut(&id) {
                if refs.remove(&r) {
                    if refs.is_empty() {
                        self.target.ref_id_index.1.remove(&id);
                    }
                    return Ok(());
                }
            }
        }
        return Err(Error::TreeBroken(
            format!("ref {r:?} not found"),
            std::backtrace::Backtrace::capture(),
        ));
    }

    fn create_id(&mut self) -> TrieId {
        self.target.auto_increment_id = self.target.auto_increment_id.inc();
        self.target.auto_increment_id
    }

    fn move_node(&mut self, id: TrieId, to: Option<(TrieId, TrieKey, C)>) -> Result<()> {
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
                    children: node.map(|n| n.children).unwrap_or_default(),
                    content: to.2,
                },
            );
        }

        Ok(())
    }

    fn do_op(&mut self, op: Op<A, C>) -> Result<LogOp<A, C>> {
        let mut undos: VecDeque<Undo<C>> = Default::default();
        let child_id = if let Some(child_id) = self.target.ref_to_id(&op.child_ref) {
            child_id.to_owned()
        } else {
            let new_id = self.create_id();
            self.create_ref(op.child_ref.to_owned(), new_id)?;
            undos.push_front(Undo::DeleteRef(op.child_ref.to_owned()));
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

        // ensures no cycles are introduced.
        if child_id != parent_id && !self.target.is_ancestor(&parent_id, &child_id) {
            let parent_node = if let Some(parent_node) = self.target.get(&parent_id) {
                parent_node
            } else {
                return Err(Error::TreeBroken(
                    format!("parent node not found {}", parent_id),
                    std::backtrace::Backtrace::capture(),
                ));
            };
            let old_node = self.target.get(&child_id).cloned();
            let conflict_mode =
                if let Some(conflict_node_id) = parent_node.children.get(&op.child_key).cloned() {
                    if conflict_node_id != child_id {
                        Some((conflict_node_id, {
                            let conflict_node = if let Some(conflict_node) =
                                self.target.get(&conflict_node_id).to_owned()
                            {
                                conflict_node
                            } else {
                                return Err(Error::TreeBroken(
                                    format!("conflict node not found {}", parent_id),
                                    std::backtrace::Backtrace::capture(),
                                ));
                            };
                            if conflict_node.children.is_empty() {
                                ConflictMode::KeepNew
                            } else {
                                if old_node
                                    .as_ref()
                                    .map(|n| n.children.is_empty())
                                    .unwrap_or(true)
                                {
                                    // new is empty
                                    ConflictMode::KeepBefore
                                } else {
                                    ConflictMode::KeepNew
                                }
                            }
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                };

            match conflict_mode {
                Some((conflict_node_id, ConflictMode::KeepBefore)) => {
                    self.redirect_ref(op.child_ref.to_owned(), conflict_node_id)?;
                    undos.push_front(Undo::RedirectRef(op.child_ref.to_owned(), child_id));

                    self.move_node(
                        child_id,
                        Some((
                            CONFLICT,
                            TrieKey(child_id.to_string()),
                            op.child_content.to_owned(),
                        )),
                    )?;
                    undos.push_front(Undo::Move {
                        id: child_id,
                        to: old_node
                            .as_ref()
                            .map(|n| (n.parent, n.key.to_owned(), n.content.to_owned())),
                    });
                }
                Some((conflict_node_id, ConflictMode::KeepNew)) => {
                    let refs = self
                        .get_refs(&conflict_node_id)
                        .map(|r| r.iter().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();

                    for r in refs {
                        self.redirect_ref(r.to_owned(), child_id)?;
                        undos.push_front(Undo::RedirectRef(r, conflict_node_id));
                    }

                    let conflict_node = if let Some(conflict_node) =
                        self.target.get(&conflict_node_id).to_owned()
                    {
                        conflict_node.clone()
                    } else {
                        return Err(Error::TreeBroken(
                            format!("conflict node not found {}", parent_id),
                            std::backtrace::Backtrace::capture(),
                        ));
                    };

                    self.move_node(
                        conflict_node_id,
                        Some((
                            CONFLICT,
                            TrieKey(conflict_node_id.to_string()),
                            conflict_node.content.to_owned(),
                        )),
                    )?;
                    undos.push_front(Undo::Move {
                        id: conflict_node_id,
                        to: Some((
                            conflict_node.parent,
                            conflict_node.key,
                            conflict_node.content,
                        )),
                    });

                    self.move_node(
                        child_id,
                        Some((
                            parent_id,
                            op.child_key.to_owned(),
                            op.child_content.to_owned(),
                        )),
                    )?;
                    undos.push_front(Undo::Move {
                        id: child_id,
                        to: old_node.map(|n| (n.parent, n.key, n.content)),
                    });
                }
                None => {
                    self.move_node(
                        child_id,
                        Some((
                            parent_id,
                            op.child_key.to_owned(),
                            op.child_content.to_owned(),
                        )),
                    )?;

                    undos.push_front(Undo::Move {
                        id: child_id,
                        to: old_node.map(|n| (n.parent, n.key, n.content)),
                    });
                }
            }
        }

        Ok(LogOp {
            op,
            undos: undos.into_iter().collect(),
        })
    }

    fn undo_op(&mut self, log: &LogOp<A, C>) -> Result<()> {
        for undo in log.undos.iter().cloned() {
            match undo {
                Undo::DeleteRef(r) => self.remove_ref(&r)?,
                Undo::RedirectRef(r, id) => self.redirect_ref(r, id)?,
                Undo::Move { id, to } => self.move_node(id, to)?,
            }
        }

        Ok(())
    }

    fn redo_op(&mut self, log: &LogOp<A, C>) -> Result<LogOp<A, C>> {
        self.do_op(log.op.clone())
    }

    fn apply(&mut self, ops: Vec<Op<A, C>>) -> Result<()> {
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
    use crdts::{CmRDT, VClock};

    use super::{Op, Trie, TrieKey, TrieRef};

    #[test]
    fn test() {
        let mut clock = VClock::new();
        let mut t = Trie::<u64, String>::default();
        let mut w = t.write();
        clock.apply(clock.inc(1));
        w.apply(vec![Op {
            clock: clock.to_owned(),
            actor: 1,
            parent_ref: TrieRef(0),
            child_key: TrieKey("abc".to_string()),
            child_ref: TrieRef(1),
            child_content: "test folder".to_string(),
        }])
        .unwrap();
        clock.apply(clock.inc(1));
        let clock_2 = clock.clone();
        clock.apply(clock.inc(1));
        w.apply(vec![Op {
            clock: clock,
            actor: 1,
            parent_ref: TrieRef(1),
            child_key: TrieKey("hello".to_string()),
            child_ref: TrieRef(2),
            child_content: "test file".to_string(),
        }])
        .unwrap();
        w.apply(vec![Op {
            clock: clock_2,
            actor: 1,
            parent_ref: TrieRef(1),
            child_key: TrieKey("hello".to_string()),
            child_ref: TrieRef(2),
            child_content: "test file 2".to_string(),
        }])
        .unwrap();
        w.commit().unwrap();
        dbg!(&t);

        println!("{t}");
    }
}
