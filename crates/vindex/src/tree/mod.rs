use std::clone;
use std::collections::{HashMap, HashSet, LinkedList};
use std::hash::Hash;

use crdts::{Actor, CmRDT, CvRDT, Dot, VClock};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TreeId(u128);

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TreeKey(String);

#[derive(Debug, Default)]
struct TreeState {
    /// parent -> key -> child
    tree: HashMap<TreeId, HashMap<TreeKey, TreeId>>,
    /// child -> parent index
    parent_map: HashMap<TreeId, TreeId>,
}

impl TreeState {
    fn rm_node(&mut self, id: &TreeId, key: &TreeKey) {
        let parent_id = self.parent_map.get(id);
        if let Some(parent_id) = parent_id {
            if let Some(map) = self.tree.get_mut(&parent_id) {
                map.remove(&key);
            }
            self.parent_map.remove(id);
        }
    }

    // removes a subtree.  useful for emptying trash.
    // not used by crdt algo.
    fn rm_subtree(&mut self, parent_id: &TreeId) {
        for (key, id) in self.children(parent_id) {
            self.rm_subtree(&id);
            self.rm_node(&id, &key);
        }
    }

    /// adds a node to the tree
    fn add_node(&mut self, parent_id: TreeId, key: TreeKey, child_id: TreeId) {
        if let Some(n) = self.tree.get_mut(&parent_id) {
            n.insert(key, child_id.to_owned());
        } else {
            let mut h = HashMap::new();
            h.insert(key, child_id.clone());
            self.tree.insert(parent_id.clone(), h);
        }
        self.parent_map.insert(child_id, parent_id);
    }

    /// returns matching node, or None.
    fn find_parent(&self, id: &TreeId) -> Option<&TreeId> {
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
        while let Some(parent_id) = self.find_parent(target_id) {
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
    timestamp: VClock<A>,
    actor: A,
    parent_id: TreeId,
    key: TreeKey,
    id: TreeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogOp<A: Actor> {
    op: Op<A>,
    old_key: TreeKey,
    old_id: TreeId,
}

#[derive(Default)]
struct Tree<A: Actor> {
    log: LinkedList<LogOp<A>>,
    state: TreeState,
}

impl<A: Actor> Tree<A> {
    fn do_op(&mut self) {
        
    }
}

impl<A: Actor> CmRDT for Tree<A> {
    type Op = Vec<Op<A>>;

    type Validation = std::convert::Infallible;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        todo!()
    }

    fn apply(&mut self, op: Self::Op) {
        
    }
}
