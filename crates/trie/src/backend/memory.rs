use std::{
    collections::{
        btree_map::Entry as BTreeMapEntry, hash_map::Entry as HashMapEntry, BTreeMap, HashMap,
        HashSet,
    },
    marker::PhantomData,
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

    log: Vec<LogOp<M, C>>,
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
                        content: Default::default(),
                    },
                ),
                (
                    CONFLICT,
                    TrieNode {
                        parent: CONFLICT,
                        key: TrieKey(Default::default()),
                        content: Default::default(),
                    },
                ),
            ]),
            children: HashMap::from([(ROOT, BTreeMap::default()), (CONFLICT, BTreeMap::default())]),
            ref_id_index: (
                HashMap::from([(TrieRef::from(0), ROOT)]),
                HashMap::from([(ROOT, HashSet::from([TrieRef::from(0)]))]),
            ),
            auto_increment_id: TrieId::from(10),
            log: Vec::new(),
        }
    }
}

pub struct TrieMemoryBackendChildrenIter<'a> {
    iter: Option<std::collections::btree_map::Iter<'a, TrieKey, TrieId>>,
}

impl<'a> Iterator for TrieMemoryBackendChildrenIter<'a> {
    type Item = Result<(&'a TrieKey, &'a TrieId)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|i| i.next().map(|i| Ok(i)))
    }
}

pub struct TrieMemoryBackendLogIter<'a, M: TrieMarker + 'a, C: TrieContent + 'a> {
    iter: Option<std::iter::Rev<std::slice::Iter<'a, LogOp<M, C>>>>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<'a, M: TrieMarker + 'a, C: TrieContent + 'a> Iterator for TrieMemoryBackendLogIter<'a, M, C> {
    type Item = Result<&'a LogOp<M, C>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.as_mut().and_then(|i| i.next().map(|i| Ok(i)))
    }
}

impl<M: TrieMarker, C: TrieContent> TrieBackend<M, C> for TrieMemoryBackend<M, C> {
    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        Ok(self.ref_id_index.0.get(&r).cloned())
    }

    type GetRefsRef<'a> = &'a TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::collections::hash_set::Iter<'a, TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Result<Option<Self::GetRefs<'_>>> {
        Ok(self.ref_id_index.1.get(&id).map(|s| s.iter()))
    }

    type Get<'a> = &'a TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Result<Option<Self::Get<'_>>> {
        Ok(self.tree.get(&id))
    }

    type GetChildrenKey<'a> = &'a TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = &'a TrieId
    where
        Self: 'a;

    type GetChildren<'a> = TrieMemoryBackendChildrenIter<'a>
    where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        Ok(TrieMemoryBackendChildrenIter {
            iter: self.children.get(&id).map(|s| s.iter()),
        })
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        Ok(self.children.get(&id).and_then(|m| m.get(&key)).cloned())
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = TrieMemoryBackendLogIter<'a, M, C>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
        Ok(TrieMemoryBackendLogIter {
            c: Default::default(),
            m: Default::default(),
            iter: Some(self.log.iter().rev()),
        })
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
    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>> {
        self.trie.get_id(r)
    }

    type GetRefsRef<'a> = &'a TrieRef
    where
        Self: 'a;

    type GetRefs<'a> = std::collections::hash_set::Iter<'a, TrieRef>
    where
        Self: 'a;

    fn get_refs(&self, id: TrieId) -> Result<Option<Self::GetRefs<'_>>> {
        self.trie.get_refs(id)
    }

    type Get<'a> = &'a TrieNode<C>
    where
        Self: 'a;

    fn get(&self, id: TrieId) -> Result<Option<Self::Get<'_>>> {
        self.trie.get(id)
    }

    type GetChildrenKey<'a> = &'a TrieKey
    where
        Self: 'a;

    type GetChildrenId<'a>  = &'a TrieId
    where
        Self: 'a;

    type GetChildren<'a> = TrieMemoryBackendChildrenIter<'a>
        where Self: 'a;

    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>> {
        self.trie.get_children(id)
    }

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>> {
        self.trie.get_child(id, key)
    }

    type IterLogItem<'a> = &'a LogOp<M, C>
    where
        Self: 'a;
    type IterLog<'a> = TrieMemoryBackendLogIter<'a, M, C>
    where
        Self: 'a;
    fn iter_log(&self) -> Result<Self::IterLog<'_>> {
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

    fn create_id(&mut self) -> Result<TrieId> {
        self.trie.auto_increment_id = self.trie.auto_increment_id.inc();
        Ok(self.trie.auto_increment_id)
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
        Ok(self.trie.log.pop())
    }

    fn push_log(&mut self, log: LogOp<M, C>) -> Result<()> {
        Ok(self.trie.log.push(log))
    }

    fn commit(self) -> Result<()> {
        Ok(())
    }

    fn rollback(self) -> Result<()> {
        todo!()
    }
}
