pub mod memory;
pub mod rocks;

use std::borrow::Borrow;

use crate::{
    Error, LogOp, Result, TrieContent, TrieHash, TrieId, TrieKey, TrieMarker, TrieNode, TrieRef,
};

pub trait TrieBackend<M: TrieMarker, C: TrieContent> {
    fn get_id(&self, r: TrieRef) -> Result<Option<TrieId>>;

    type GetRefsRef<'a>: Borrow<TrieRef> + 'a
    where
        Self: 'a;
    type GetRefs<'a>: Iterator<Item = Self::GetRefsRef<'a>>
    where
        Self: 'a;
    fn get_refs(&self, id: TrieId) -> Result<Option<Self::GetRefs<'_>>>;

    type Get<'a>: Borrow<TrieNode<C>> + 'a
    where
        Self: 'a;
    fn get(&self, id: TrieId) -> Result<Option<Self::Get<'_>>>;

    type GetChildrenKey<'a>: Borrow<TrieKey> + 'a
    where
        Self: 'a;
    type GetChildrenId<'a>: Borrow<TrieId> + 'a
    where
        Self: 'a;
    type GetChildren<'a>: Iterator<Item = Result<(Self::GetChildrenKey<'a>, Self::GetChildrenId<'a>)>>
    where
        Self: 'a;
    fn get_children(&self, id: TrieId) -> Result<Self::GetChildren<'_>>;

    fn get_child(&self, id: TrieId, key: TrieKey) -> Result<Option<TrieId>>;

    type IterLogItem<'a>: Borrow<LogOp<M, C>> + 'a
    where
        Self: 'a;
    type IterLog<'a>: Iterator<Item = Self::IterLogItem<'a>>
    where
        Self: 'a;
    fn iter_log(&self) -> Self::IterLog<'_>;

    fn get_ensure(&self, id: TrieId) -> Result<Self::Get<'_>> {
        self.get(id)?
            .ok_or(Error::TreeBroken(format!("Trie id {id} not found")))
    }

    fn is_ancestor(&self, child_id: TrieId, ancestor_id: TrieId) -> Result<bool> {
        let mut target_id = child_id;
        while let Some(node) = self.get(target_id)? {
            if node.borrow().parent == ancestor_id {
                return Ok(true);
            }
            target_id = node.borrow().parent;
            if target_id.id() < 10 {
                break;
            }
        }
        Ok(false)
    }

    type Writer<'a>: TrieBackendWriter<'a, M, C>
    where
        Self: 'a;
    fn write<'a>(&'a mut self) -> Result<Self::Writer<'a>>;
}

pub trait TrieBackendWriter<'a, M: TrieMarker, C: TrieContent>: TrieBackend<M, C> {
    fn set_hash(&mut self, id: TrieId, hash: TrieHash) -> Result<()>;

    fn set_ref(&mut self, r: TrieRef, id: Option<TrieId>) -> Result<Option<TrieId>>;

    fn create_id(&mut self) -> Result<TrieId>;

    fn set_tree_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>>;

    fn pop_log(&mut self) -> Result<Option<LogOp<M, C>>>;
    fn push_log(&mut self, log: LogOp<M, C>) -> Result<()>;
}
