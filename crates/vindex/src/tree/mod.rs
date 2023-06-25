use std::clone;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, LinkedList};
use std::fmt::Debug;
use std::hash::Hash;

use crdts::ctx::ReadCtx;
use crdts::{Actor, CmRDT, CvRDT, Dot, VClock};
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TreeId(u128);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TreeRef(Uuid);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TreeKey(String);

#[derive(Debug, Default)]
struct TreeState {
    /// parent -> key -> child
    tree: HashMap<TreeId, HashMap<TreeKey, TreeId>>,
    /// child -> (parent, child key) index
    parent_map: HashMap<TreeId, (TreeId, TreeKey)>,
}

impl TreeState {
    fn rm_node(&mut self, id: &TreeId) {
        debug_assert!(!self.tree.contains_key(id), "Tree structure is broken");
        let parent_id = self.parent_map.get(id);
        if let Some((parent_id, key)) = parent_id {
            if let Some(map) = self.tree.get_mut(parent_id) {
                map.remove(key);
                if map.is_empty() {
                    self.tree.remove(parent_id);
                }
            }
            self.parent_map.remove(id);
        }
    }

    // removes a subtree.  useful for emptying trash.
    // not used by crdt algo.
    // fn rm_subtree(&mut self, parent_id: &TreeId) {
    //     for (key, id) in self.children(parent_id) {
    //         self.rm_subtree(&id);
    //         self.rm_node(&id);
    //     }
    // }

    /// adds a node to the tree
    fn add_node(&mut self, parent_id: TreeId, key: TreeKey, child_id: TreeId) {
        debug_assert!(!self.parent_map.contains_key(&parent_id), "Tree structure is broken");
        if let Some(n) = self.tree.get_mut(&parent_id) {
            n.insert(key.clone(), child_id.to_owned());
        } else {
            let mut h = HashMap::new();
            h.insert(key.clone(), child_id.clone());
            self.tree.insert(parent_id.clone(), h);
        }
        self.parent_map.insert(child_id, (parent_id, key));
    }

    /// returns matching node, or None.
    fn find_parent(&self, id: &TreeId) -> Option<&(TreeId, TreeKey)> {
        self.parent_map.get(id)
    }

    /// returns children (IDs) of a given parent node.
    /// useful for walking tree.
    /// not used by crdt algo.
    fn children(&self, parent_id: &TreeId) -> Vec<(TreeKey, TreeId)> {
        if let Some(list) = self.tree.get(parent_id) {
            list.iter().map(|(a, b)| (a.clone(), b.clone())).collect()
        } else {
            Vec::<(TreeKey, TreeId)>::default()
        }
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
    fn is_ancestor(&self, child_id: &TreeId, ancestor_id: &TreeId) -> bool {
        let mut target_id = child_id;
        while let Some((parent_id, _)) = self.find_parent(target_id) {
            if parent_id == ancestor_id {
                return true;
            }
            target_id = parent_id;
        }
        false
    }

    /// Total number of nodes in the tree
    fn num_nodes(&self) -> usize {
        self.parent_map.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op<A: Actor> {
    clock: VClock<A>,
    actor: A,
    parent_id: TreeId,
    child_key: TreeKey,
    child_id: TreeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogOp<A: Actor> {
    op: Op<A>,
    old_parent: Option<(TreeId, TreeKey)>,
}

#[derive(Default)]
struct Tree<A: Actor> {
    log: LinkedList<LogOp<A>>,
    state: TreeState,
    clock: VClock<A>,
}

impl<A: Actor> Tree<A> {
    pub fn new_move(
        actor: A,
        clock: VClock<A>,
        parent_id: TreeId,
        child_key: TreeKey,
        child_id: TreeId,
    ) -> Op<A> {
        Op {
            actor,
            clock,
            parent_id,
            child_key,
            child_id,
        }
    }

    fn do_op(&mut self, op: Op<A>) -> LogOp<A> {
        let old_parent = self.state.find_parent(&op.child_id).cloned();

        // ensures no cycles are introduced.
        if !(op.child_id == op.parent_id || self.state.is_ancestor(&op.parent_id, &op.child_id)) {
            self.state.rm_node(&op.child_id);

            self.state.add_node(
                op.parent_id.clone(),
                op.child_key.clone(),
                op.child_id.clone(),
            );
        }

        LogOp { op, old_parent }
    }

    fn undo_op(&mut self, log: &LogOp<A>) {
        self.state.rm_node(&log.op.child_id);

        if let Some((old_parent_id, old_key)) = &log.old_parent {
            self.state.add_node(
                old_parent_id.to_owned(),
                old_key.to_owned(),
                log.op.child_id.to_owned(),
            );
        }
    }

    pub fn redo_op(&mut self, log: &LogOp<A>) -> LogOp<A> {
        let op = log.op.clone();
        self.do_op(op)
    }
}

impl<A: Actor + Debug> CmRDT for Tree<A> {
    type Op = Op<A>;

    type Validation = std::convert::Infallible;

    fn validate_op(&self, _: &Self::Op) -> Result<(), Self::Validation> {
        return Ok(());
    }

    fn apply(&mut self, op: Self::Op) {
        self.clock.merge(op.clock.clone());
        if let Some(last) = self.log.pop_front() {
            match op.clock.partial_cmp(&last.op.clock) {
                None | Some(Ordering::Equal) => {
                    // This case should never happen in normal operation
                    // because it is requirement/invariant that all
                    // timestamps are unique.  However, uniqueness is not
                    // strictly enforced in this impl.
                    // The crdt paper does not even check for this case.
                    // We just treat it as a no-op.
                    panic!("op with timestamp equal to previous op ignored. (not applied).  Every op must have a unique timestamp.");
                }
                Some(Ordering::Less) => {
                    self.undo_op(&last);
                    self.apply(op);
                    self.redo_op(&last);
                    self.log.push_front(last);
                }
                Some(Ordering::Greater) => {
                    let log = self.do_op(op);
                    self.log.push_front(log);
                }
            }
        } else {
            let log = self.do_op(op);
            self.log.push_front(log);
        }
    }
}
