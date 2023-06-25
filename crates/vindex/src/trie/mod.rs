/// Tree node id
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieId(u128);

/// The key of the tree
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieKey(String);

/// The reference of the node, which is used to determine the node of the operation during the distributed operation
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieRef(u128);

struct TrieNode {
    parent: TreeId,
    refs: Vec<TrieRef>,
    children: HashMap<TreeKey, TreeId>,
}

#[derive(Debug, Default)]
struct TrieState {
    /// id -> node
    tree: HashMap<TreeId, TrieNode>,
    /// ref -> id index
    ref_id_map: HashMap<TrieRef, TreeId>,
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
}
