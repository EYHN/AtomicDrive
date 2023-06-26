use std::{
    cmp::Ordering,
    collections::{hash_map::Entry as HashMapEntry, HashMap, LinkedList, VecDeque},
    fmt::Display,
};

use crdts::{Actor, CmRDT, VClock};
use std::fmt::Debug;
use thiserror::Error;
use utils::tree_stringify;

use std::hash::Hash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Operation may break the tree, or the tree is already broken. message: `{0}`")]
    TreeBroken(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait TrieContent: Clone + Hash + Default {}
impl<A: Clone + Hash + Default> TrieContent for A {}

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
    /// old parent id, old child key, old content
    from: Option<(TrieId, TrieKey, C)>,
    created_ref: bool,
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
    /// ref -> id index
    ref_id_map: HashMap<TrieRef, TrieId>,

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
            tree: HashMap::from([(
                TrieId(0),
                TrieNode {
                    parent: TrieId(0),
                    key: TrieKey(Default::default()),
                    hash: Default::default(),
                    children: Default::default(),
                    content: Default::default(),
                },
            )]),
            ref_id_map: HashMap::from([(TrieRef(0), TrieId(0))]),
            auto_increment_id: TrieId(0),
            log: LinkedList::new(),
        }
    }
}

impl<A: Actor, C: TrieContent> Trie<A, C> {
    fn ref_to_id(&self, r: &TrieRef) -> Option<&TrieId> {
        self.ref_id_map.get(r)
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
    fn create_id(&mut self, r: TrieRef) -> Result<TrieId> {
        self.target.auto_increment_id = self.target.auto_increment_id.inc();
        if self
            .target
            .ref_id_map
            .insert(r.to_owned(), self.target.auto_increment_id)
            .is_some()
        {
            return Err(Error::TreeBroken(format!("ref {r:?} already exsits")));
        }
        Ok(self.target.auto_increment_id)
    }

    fn rm_node(&mut self, id: &TrieId) -> Result<TrieNode<C>> {
        let node = if let Some(node) = self.target.tree.remove(id) {
            node
        } else {
            return Err(Error::TreeBroken(format!("node id {id:?} not found")));
        };

        if let Some(parent) = self.target.tree.get_mut(&node.parent) {
            if parent.children.remove(&node.key).is_none() {
                return Err(Error::TreeBroken(format!("bad state")));
            }
        }
        Ok(node)
    }

    fn add_node(
        &mut self,
        parent_id: TrieId,
        key: TrieKey,
        child_id: TrieId,
        children: HashMap<TrieKey, TrieId>,
        content: C,
    ) -> Result<()> {
        if let Some(n) = self.target.tree.get_mut(&parent_id) {
            match n.children.entry(key.to_owned()) {
                HashMapEntry::Occupied(_) => {
                    return Err(Error::TreeBroken(format!("key {key:?} occupied")));
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(child_id);
                }
            }
        } else {
            return Err(Error::TreeBroken(format!(
                "node id {parent_id:?} not found"
            )));
        }
        self.target.tree.insert(
            child_id,
            TrieNode {
                parent: parent_id,
                key,
                hash: Default::default(),
                children,
                content,
            },
        );
        Ok(())
    }

    fn do_op(&mut self, op: Op<A, C>) -> Result<LogOp<A, C>> {
        let mut created_ref = false;
        let child_id = if let Some(child_id) = self.target.ref_to_id(&op.child_ref) {
            child_id.to_owned()
        } else {
            created_ref = true;
            self.create_id(op.child_ref.to_owned())?
        };
        let parent_id = if let Some(parent_id) = self.target.ref_to_id(&op.parent_ref) {
            parent_id.to_owned()
        } else {
            return Err(Error::TreeBroken(format!(
                "parent ref {:?} not found",
                &op.parent_ref
            )));
        };
        let old_node = self.target.get(&child_id);
        let old = old_node.map(|n| (n.parent, n.key.to_owned(), n.content.to_owned()));
        let has_old_node = old_node.is_some();

        // ensures no cycles are introduced.
        if child_id != parent_id
            && (!has_old_node || !self.target.is_ancestor(&parent_id, &child_id))
        {
            let old_node = if has_old_node {
                Some(self.rm_node(&child_id)?)
            } else {
                None
            };

            self.add_node(
                parent_id,
                op.child_key.to_owned(),
                child_id,
                old_node.map(|n| n.children).unwrap_or_default(),
                op.child_content.to_owned(),
            )?;
        }

        Ok(LogOp {
            op,
            from: old,
            created_ref,
        })
    }

    fn undo_op(&mut self, log: &LogOp<A, C>) -> Result<()> {
        let child_id = if let Some(child_id) = self.target.ref_to_id(&log.op.child_ref) {
            child_id.to_owned()
        } else {
            return Err(Error::TreeBroken(format!(
                "ref {:?} not found",
                log.op.child_ref
            )));
        };

        let node = self.rm_node(&child_id)?;

        if let Some((old_parent_id, old_key, old_content)) = &log.from {
            self.add_node(
                old_parent_id.to_owned(),
                old_key.to_owned(),
                child_id.to_owned(),
                node.children,
                old_content.to_owned(),
            )?;
        }

        Ok(())
    }

    fn redo_op(&mut self, log: &LogOp<A, C>) -> Result<LogOp<A, C>> {
        self.do_op(
            log.clock.to_owned(),
            log.actor.to_owned(),
            log.parent_id,
            log.child_key.to_owned(),
            log.child_id,
            log.child_content.to_owned(),
        )
    }

    fn apply(&mut self, ops: Vec<Op<A, C>>) -> Result<()> {
        let mut redo_queue = VecDeque::new();
        if let Some(first_op) = ops.first() {
            loop {
                if let Some(last) = self.target.log.pop_back() {
                    match first_op.clock.partial_cmp(&last.clock) {
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
                    match op.clock.partial_cmp(&redo.clock) {
                        None | Some(Ordering::Equal) => {
                            panic!("op with timestamp equal to previous op ignored. (not applied).  Every op must have a unique timestamp.");
                        }
                        Some(Ordering::Less) => {
                            let log_op = self.do_op(
                                op.clock,
                                op.actor,
                                parent_id,
                                op.child_key,
                                child_id,
                                op.child_content,
                            )?;
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
                    let log_op = self.do_op(
                        op.clock,
                        op.actor,
                        parent_id,
                        op.child_key,
                        child_id,
                        op.child_content,
                    )?;
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
        w.apply(vec![Op {
            clock: clock.to_owned(),
            actor: 1,
            parent_ref: TrieRef(1),
            child_key: TrieKey("hello".to_string()),
            child_ref: TrieRef(2),
            child_content: "test file".to_string(),
        }])
        .unwrap();
        w.commit().unwrap();

        clock.apply(clock.inc(1));
        w.apply(vec![Op {
            clock: clock.to_owned(),
            actor: 1,
            parent_ref: TrieRef(0),
            child_key: TrieKey("hello".to_string()),
            child_ref: TrieRef(1),
            child_content: "test folder".to_string(),
        }])
        .unwrap();
        w.commit().unwrap();

        clock.apply(clock.inc(1));
        w.apply(vec![Op {
            clock: clock.to_owned(),
            actor: 1,
            parent_ref: TrieRef(2),
            child_key: TrieKey("hello".to_string()),
            child_ref: TrieRef(1),
            child_content: "test folder".to_string(),
        }])
        .unwrap();
        w.commit().unwrap();
        println!("{t}");
    }
}
