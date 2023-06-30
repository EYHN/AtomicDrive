use std::collections::{
    btree_map::Entry as BTreeMapEntry, hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet,
    LinkedList,
};

use crate::{
    Error, LogOp, Result, TrieContent, TrieHash, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef,
    CONFLICT, ROOT,
};

use super::{TrieBackend, TrieBackendWriter};

#[derive(Clone, PartialEq, Eq)]
pub struct TrieMemoryBackend<M: TrieMarker, C: TrieContent> {
    /// id -> node
    tree: HashMap<TrieId, TrieNode<C>>,

    children: HashMap<TrieId, BTreeMap<TrieKey, TrieId>>,
    /// ref <-> id index
    ref_id_index: (HashMap<TrieRef, TrieId>, HashMap<TrieId, HashSet<TrieRef>>),

    auto_increment_id: TrieId,

    log: LinkedList<LogOp<M, C>>,
}

impl<M: TrieMarker, C: TrieContent> Default for TrieMemoryBackend<M, C> {
    fn default() -> Self {
        TrieMemoryBackend {
            tree: HashMap::from([
                (
                    ROOT,
                    TrieNode {
                        parent: ROOT,
                        key: TrieKey(Default::default()),
                        hash: TrieHash::expired(),
                        content: Default::default(),
                    },
                ),
                (
                    CONFLICT,
                    TrieNode {
                        parent: CONFLICT,
                        key: TrieKey(Default::default()),
                        hash: TrieHash::expired(),
                        content: Default::default(),
                    },
                ),
            ]),
            children: HashMap::from([(ROOT, BTreeMap::default()), (CONFLICT, BTreeMap::default())]),
            ref_id_index: (
                HashMap::from([(TrieRef(0), ROOT)]),
                HashMap::from([(ROOT, HashSet::from([TrieRef(0)]))]),
            ),
            auto_increment_id: TrieId(10),
            log: LinkedList::new(),
        }
    }
}

impl<M: TrieMarker, C: TrieContent> TrieBackend<M, C> for TrieMemoryBackend<M, C> {
    fn get_id(&self, r: TrieRef) -> Option<TrieId> {
        self.ref_id_index.0.get(&r).cloned()
    }

    type GetRefsRef<'a> = &'a TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::collections::hash_set::Iter<'a, TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Option<Self::GetRefs<'_>> {
        self.ref_id_index.1.get(&id).map(|s| s.iter())
    }

    type Get<'a> = &'a TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Option<Self::Get<'_>> {
        self.tree.get(&id)
    }

    type GetChildrenKey<'a> = &'a TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = &'a TrieId
    where
        Self: 'a;

    type GetChildren<'a> = std::collections::btree_map::Iter<'a, TrieKey, TrieId>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Option<Self::GetChildren<'_>> {
        self.children.get(&id).map(|s| s.iter())
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Option<TrieId> {
        self.children.get(&id).and_then(|m| m.get(&key)).cloned()
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = std::iter::Rev<std::collections::linked_list::Iter<'a, LogOp<M, C>>>
    where
        Self: 'a;
    fn iter_log(&self) -> Self::IterLog<'_> {
        self.log.iter().rev()
    }

    type Writer<'a> = TrieMemoryBackendWriter<'a, M, C>
    where Self: 'a;

    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>> {
        Ok(Self::Writer { trie: self })
    }
}

pub struct TrieMemoryBackendWriter<'a, M: TrieMarker, C: TrieContent> {
    trie: &'a mut TrieMemoryBackend<M, C>,
}

impl<M: TrieMarker, C: TrieContent> TrieBackend<M, C> for TrieMemoryBackendWriter<'_, M, C> {
    fn get_id(&self, r: TrieRef) -> Option<TrieId> {
        self.trie.get_id(r)
    }

    type GetRefsRef<'a> = &'a TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::collections::hash_set::Iter<'a, TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Option<Self::GetRefs<'_>> {
        self.trie.get_refs(id)
    }

    type Get<'a> = &'a TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Option<Self::Get<'_>> {
        self.trie.get(id)
    }

    type GetChildrenKey<'a> = &'a TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = &'a TrieId
    where
        Self: 'a;

    type GetChildren<'a> = std::collections::btree_map::Iter<'a, TrieKey, TrieId>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Option<Self::GetChildren<'_>> {
        self.trie.get_children(id)
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Option<TrieId> {
        self.trie.get_child(id, key)
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = std::iter::Rev<std::collections::linked_list::Iter<'a, LogOp<M, C>>>
    where
        Self: 'a;
    fn iter_log(&self) -> Self::IterLog<'_> {
        self.trie.iter_log()
    }

    type Writer<'a> = TrieMemoryBackendWriter<'a, M, C>
    where Self: 'a;

    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>> {
        Err(Error::InvalidOp("not support".to_string()))
    }
}

impl<'a, M: TrieMarker, C: TrieContent> TrieBackendWriter<'a, M, C>
    for TrieMemoryBackendWriter<'a, M, C>
{
    fn set_hash(&mut self, id: TrieId, hash: TrieHash) -> Result<()> {
        if let Some(current) = self.trie.tree.get_mut(&id) {
            current.hash = hash;
            Ok(())
        } else {
            Err(Error::TreeBroken(format!("id {id} not found")))
        }
    }

    fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>> {
        let old_id = if let Some(id) = self.trie.ref_id_index.0.remove(&r) {
            if let Some(refs) = self.trie.ref_id_index.1.get_mut(&id) {
                if refs.remove(&r) {
                    if refs.is_empty() {
                        self.trie.ref_id_index.1.remove(&id);
                    }
                }
            }
            Some(id)
        } else {
            None
        };
        if let Some(id) = id {
            self.trie.ref_id_index.0.insert(r.to_owned(), id);
            match self.trie.ref_id_index.1.entry(id) {
                HashMapEntry::Occupied(mut entry) => {
                    entry.get_mut().insert(r.to_owned());
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(HashSet::from([r]));
                }
            }
        }
        Ok(old_id)
    }

    fn create_id(&mut self) -> TrieId {
        self.trie.auto_increment_id = self.trie.auto_increment_id.inc();
        self.trie.auto_increment_id
    }

    fn set_tree_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        let node = self.trie.tree.remove(&id);

        if let Some(node) = &node {
            if let Some(parent_children) = self.trie.children.get_mut(&node.parent) {
                if parent_children.remove(&node.key).is_none() {
                    return Err(Error::TreeBroken(format!("bad state")));
                }
            }
        }

        if let Some(to) = to {
            if let Some(n) = self.trie.children.get_mut(&to.0) {
                match n.entry(to.1.to_owned()) {
                    BTreeMapEntry::Occupied(_) => {
                        return Err(Error::TreeBroken(format!("key {:?} occupied", to.1)));
                    }
                    BTreeMapEntry::Vacant(entry) => {
                        entry.insert(id);
                    }
                }
            } else {
                return Err(Error::TreeBroken(format!("node id {:?} not found", to.0)));
            }

            self.trie.tree.insert(
                id,
                TrieNode {
                    parent: to.0,
                    key: to.1,
                    hash: TrieHash::expired(),
                    content: to.2,
                },
            );

            if self.trie.children.get(&id).is_none() {
                self.trie.children.insert(id, Default::default());
            }
        }

        Ok(node.map(|n| (n.parent, n.key, n.content)))
    }

    fn pop_log(&mut self) -> Result<Option<LogOp<M, C>>> {
        Ok(self.trie.log.pop_back())
    }

    fn push_log(&mut self, log: LogOp<M, C>) -> Result<()> {
        Ok(self.trie.log.push_back(log))
    }
}
