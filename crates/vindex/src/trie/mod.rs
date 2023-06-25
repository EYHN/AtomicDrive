use std::{
    collections::{HashMap, LinkedList},
    ops::Deref,
};

use crdts::{Actor, VClock};
use thiserror::Error;

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

impl TrieId {
    pub fn inc(&self) -> Self {
        TrieId(self.0 + 1)
    }
}

/// The key of the tree
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieKey(String);

/// The reference of the node, which is used to determine the node of the operation during the distributed operation
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct TrieRef(u128);

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct TrieHash([u8; 32]);

#[derive(Debug, Clone, Eq, PartialEq)]
struct TrieNode<C> {
    parent: TrieId,
    key: TrieKey,
    refs: Vec<TrieRef>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogOp<A: Actor, C: TrieContent> {
    op: Op<A, C>,
    old_parent: Option<(TrieId, TrieKey)>,
}

#[derive(Debug)]
struct Trie<A: Actor, C: TrieContent> {
    /// id -> node
    tree: HashMap<TrieId, TrieNode<C>>,
    /// ref -> id index
    ref_id_map: HashMap<TrieRef, TrieId>,

    auto_increment_id: TrieId,

    log: LinkedList<LogOp<A, C>>,
}

impl<A: Actor, C: TrieContent> Default for Trie<A, C> {
    fn default() -> Self {
        Trie {
            tree: HashMap::from([(
                TrieId(0),
                TrieNode {
                    parent: TrieId(0),
                    key: TrieKey(Default::default()),
                    refs: vec![TrieRef(0)],
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
        }
        false
    }
}

struct TrieUpdater<'a, A: Actor, C: TrieContent> {
    target: &'a mut Trie<A, C>,
}

impl<A: Actor, C: TrieContent> TrieUpdater<'_, A, C> {
    fn rm_node(&mut self, id: &TrieId) -> Result<()> {
        let (parent_id, key) = if let Some(node) = self.target.get(id) {
            (node.parent, node.key.clone())
        } else {
            return Err(Error::TreeBroken(format!("node id {id:?} not found")));
        };

        if let Some(parent) = self.target.tree.get_mut(&parent_id) {
            if parent.children.remove(&key).is_none() {
                return Err(Error::TreeBroken(format!("bad state")));
            }
        }
        self.target.tree.remove(id);
        Ok(())
    }

    fn add_node(&mut self, parent_id: TreeId, key: TreeKey, child_id: TreeId) {
        debug_assert!(
            !self.parent_map.contains_key(&parent_id),
            "Tree structure is broken"
        );
        if let Some(n) = self.tree.get_mut(&parent_id) {
            n.insert(key.clone(), child_id.to_owned());
        } else {
            let mut h = HashMap::new();
            h.insert(key.clone(), child_id.clone());
            self.tree.insert(parent_id.clone(), h);
        }
        self.parent_map.insert(child_id, (parent_id, key));
    }

    fn do_op(&mut self, op: Op<A, C>) -> Result<LogOp<A, C>> {
        let child_id = self.target.ref_to_id(&op.child_ref).unwrap_or_else(|| {
            self.target.auto_increment_id = self.target.auto_increment_id.inc();
            &self.target.auto_increment_id
        });
        let parent_id = if let Some(parent_id) = self.target.ref_to_id(&op.parent_ref) {
            parent_id
        } else {
            return Err(Error::TreeBroken(format!(
                "parent ref {:?} not found",
                &op.parent_ref
            )));
        };
        let old_node = self.target.get(&child_id);

        // ensures no cycles are introduced.
        if child_id != parent_id
            && (old_node.is_none() || !self.target.is_ancestor(&parent_id, &child_id))
        {
            self.target.rm_node(&op.child_id);

            self.target.add_node(
                op.parent_id.clone(),
                op.child_key.clone(),
                op.child_id.clone(),
            );
        }

        LogOp { op, old_parent }
    }

    fn undo_op(&mut self, log: &LogOp<A, C>) {
        self.state.rm_node(&log.op.child_id);

        if let Some((old_parent_id, old_key)) = &log.old_parent {
            self.state.add_node(
                old_parent_id.to_owned(),
                old_key.to_owned(),
                log.op.child_id.to_owned(),
            );
        }
    }

    pub fn redo_op(&mut self, log: &LogOp<A, C>) -> LogOp<A, C> {
        let op = log.op.clone();
        self.do_op(op)
    }
}

// struct TrieTransaction<'a, A: Actor, C: TrieContent> {
//     state: &'a mut TrieNode<C>,
//     ops: Op<A, C>,
// }

// impl<A: Actor, C: TrieContent> Deref for TrieTransaction<'_, A, C> {
//     type Target = TrieNode<C>;

//     fn deref(&self) -> &Self::Target {
//         self.state
//     }
// }

// impl<A: Actor, C: TrieContent> TrieTransaction<'_, A, C> {
//     fn append_ops
// }

// impl TrieState {
// fn add_node(
//     &mut self,
//     parent_id: TrieId,
//     key: TrieKey,
//     child_id: TrieId,
//     child_refs: Vec<TrieRef>,
// ) -> Result<()> {
//     if !self.tree.contains_key(&parent_id) {
//         return Err(Error::TreeBroken(format!(
//             "parent id {parent_id:?} not found"
//         )));
//     }
//     if let Some(n) = self.tree.get_mut(&parent_id) {
//         match n.children.entry(key.clone()) {
//             Entry::Occupied(o) => {
//                 return Err(Error::TreeBroken(format!("key {key:?} already exsits")));
//             }
//             Entry::Vacant(v) => v.insert(child_id.to_owned()),
//         };
//     } else {
//         let mut h = HashMap::new();
//         h.insert(key.clone(), child_id.clone());
//         self.tree.insert(parent_id.clone(), h);
//     }
//     self.tree.insert(child_id, (parent_id, key));

//     return Ok(());
// }
// }
