pub mod backend;

use std::{borrow::Borrow, cmp::Ordering, fmt::Display, marker::PhantomData};

use backend::{TrieBackend, TrieBackendWriter};
use sha2::{Digest, Sha256};
use std::fmt::Debug;
use thiserror::Error;
use utils::tree_stringify;
use uuid::Uuid;

use std::hash::Hash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Operation may break the tree, or the tree is already broken. {0}")]
    TreeBroken(String),
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait TrieContent: Clone + Hash + Default {
    fn digest(&self, d: &mut impl Digest);
}

impl TrieContent for String {
    fn digest(&self, d: &mut impl Digest) {
        d.update(self.as_bytes())
    }
}

impl TrieContent for u64 {
    fn digest(&self, d: &mut impl Digest) {
        d.update(&self.to_be_bytes())
    }
}

pub trait TrieMarker: PartialOrd + Clone + Hash {}
impl<A: PartialOrd + Clone + Hash> TrieMarker for A {}

pub const ROOT: TrieId = TrieId(0);
pub const CONFLICT: TrieId = TrieId(1);

/// Tree node id
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TrieId(pub u64);

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
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TrieKey(pub String);

impl Display for TrieKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// The reference of the node, which is used to determine the node of the operation during the distributed operation
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct TrieRef(pub u128);

impl TrieRef {
    pub fn new() -> Self {
        TrieRef(Uuid::new_v4().to_u128_le())
    }
}

impl Display for TrieRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq, Hash)]
pub struct TrieHash(pub [u8; 32]);

impl TrieHash {
    pub fn is_expired(&self) -> bool {
        self.0 == [0u8; 32]
    }

    pub fn expired() -> Self {
        Self([0u8; 32])
    }
}

impl AsRef<[u8]> for TrieHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

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
pub struct TrieNode<C> {
    pub parent: TrieId,
    pub key: TrieKey,
    pub hash: TrieHash,
    pub content: C,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op<M: TrieMarker, C: TrieContent> {
    pub marker: M,
    pub parent_ref: TrieRef,
    pub child_key: TrieKey,
    pub child_ref: TrieRef,
    pub child_content: C,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogOp<M: TrieMarker, C: TrieContent> {
    pub op: Op<M, C>,
    pub undos: Vec<Undo<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Do<C: TrieContent> {
    Ref(TrieRef, Option<TrieId>),
    Move {
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Undo<C: TrieContent> {
    Ref(TrieRef, Option<TrieId>),
    Move {
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    },
}

#[derive(Clone, PartialEq, Eq)]
pub struct Trie<M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> {
    backend: B,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker, C: TrieContent + Display, B: TrieBackend<M, C>> Display for Trie<M, C, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        self.dbg_itemization(TrieId(0), "", &mut items);

        f.write_str(&tree_stringify(
            items.iter().map(|(path, _, node)| {
                (
                    path.as_ref(),
                    if node.content.to_string().is_empty() {
                        "".to_string()
                    } else {
                        format!("[{}]", node.content.to_string())
                    },
                )
            }),
            "/",
        ))
    }
}

impl<M: TrieMarker, C: TrieContent + Display, B: TrieBackend<M, C>> Debug for Trie<M, C, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        self.dbg_itemization(TrieId(0), "", &mut items);

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

impl<M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> Trie<M, C, B> {
    pub fn new(backend: B) -> Self {
        Trie {
            backend,
            m: Default::default(),
            c: Default::default(),
        }
    }

    pub fn get_refs(&self, id: TrieId) -> Option<impl Iterator<Item = impl Borrow<TrieRef> + '_>> {
        self.backend.get_refs(id)
    }

    pub fn get(&self, id: TrieId) -> Option<impl Borrow<TrieNode<C>> + '_> {
        self.backend.get(id)
    }

    pub fn get_children(
        &self,
        id: TrieId,
    ) -> Option<impl Iterator<Item = (impl Borrow<TrieKey> + '_, impl Borrow<TrieId> + '_)>> {
        self.backend.get_children(id)
    }

    pub fn get_child(&self, id: TrieId, key: TrieKey) -> Option<TrieId> {
        self.backend.get_child(id, key)
    }

    pub fn write(&mut self) -> Result<TrieUpdater<'_, M, C, B>> {
        Ok(TrieUpdater {
            writer: self.backend.write()?,
        })
    }

    fn dbg_itemization(
        &self,
        root: TrieId,
        prefix: &str,
        base: &mut Vec<(String, TrieId, TrieNode<C>)>,
    ) {
        let node = self.backend.get_ensure(root).unwrap();
        let children = self.backend.get_children_ensure(root).unwrap();
        let path = format!("{}/{}", prefix, node.borrow().key);
        base.push((path.clone(), root, node.borrow().clone()));

        for (_, id) in children {
            self.dbg_itemization(*id.borrow(), &path, base)
        }
    }
}

pub struct TrieUpdater<'a, M: TrieMarker, C: TrieContent, B: TrieBackend<M, C> + 'a> {
    writer: B::Writer<'a>,
}

impl<M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> TrieUpdater<'_, M, C, B> {
    fn invalidate_hash(&mut self, mut id: TrieId) -> Result<()> {
        loop {
            let current = self.writer.get_ensure(id)?;
            if current.borrow().hash.is_expired() {
                break;
            } else {
                let parent = current.borrow().parent;
                core::mem::drop(current);

                self.writer.set_hash(id, TrieHash::expired())?;

                if parent == id {
                    break;
                } else {
                    id = parent;
                }
            }
        }

        Ok(())
    }

    fn calculate_hash(&mut self, root: TrieId) -> Result<()> {
        let mut search_pass = Vec::from([root]);
        let mut calculate_pass = Vec::from([]);

        while let Some(current) = search_pass.pop() {
            let current_node_children = self.writer.get_children_ensure(current)?;

            for (_, child_id) in current_node_children {
                let child = self.writer.get_ensure(*child_id.borrow())?;

                if child.borrow().hash.is_expired() {
                    search_pass.push(*child_id.borrow());
                }
            }

            calculate_pass.push(current)
        }

        while let Some(current) = calculate_pass.pop() {
            let current_node = self.writer.get_ensure(current)?;
            let current_node_children = self.writer.get_children_ensure(current)?;

            let mut hasher = Sha256::new();

            let mut children_len: u64 = 0;

            for (key, child_id) in current_node_children {
                let child = self.writer.get_ensure(*child_id.borrow())?;

                hasher.update(&key.borrow().0.as_bytes());

                hasher.update(&key.borrow().0.len().to_be_bytes());

                hasher.update(&b"|");

                hasher.update(&child.borrow().hash);

                hasher.update(&b"|");

                children_len += 1;
            }

            hasher.update(&children_len.to_be_bytes());

            hasher.update(&b"|");

            current_node.borrow().content.digest(&mut hasher);

            core::mem::drop(current_node);

            let hash = hasher.finalize();
            self.writer
                .set_hash(current, TrieHash(hash[0..32].try_into().unwrap()))?;
        }

        Ok(())
    }

    fn move_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        if let Some((to_parent_id, _, _)) = &to {
            self.invalidate_hash(*to_parent_id)?;
        }
        let old = self.writer.set_tree_node(id, to)?;
        if let Some((old_parent_id, _, _)) = &old {
            self.invalidate_hash(*old_parent_id)?;
        }
        Ok(old)
    }

    fn do_op(&mut self, op: Op<M, C>) -> Result<LogOp<M, C>> {
        let mut dos: Vec<Do<C>> = Vec::with_capacity(3);
        let child_id = if let Some(child_id) = self.writer.get_id(op.child_ref.to_owned()) {
            child_id.to_owned()
        } else {
            let new_id = self.writer.create_id();
            dos.push(Do::Ref(op.child_ref.to_owned(), Some(new_id)));
            new_id
        };
        let parent_id = if let Some(parent_id) = self.writer.get_id(op.parent_ref.to_owned()) {
            parent_id.to_owned()
        } else {
            return Err(Error::TreeBroken(format!(
                "parent ref {:?} not found",
                &op.parent_ref
            )));
        };

        // ensures no cycles are introduced.
        'c: {
            if child_id != parent_id && !self.writer.is_ancestor(parent_id, child_id) {
                if let Some(conflict_node_id) =
                    self.writer.get_child(parent_id, op.child_key.to_owned())
                {
                    if conflict_node_id != child_id {
                        let conflict_node = self.writer.get_ensure(conflict_node_id)?;
                        let conflict_is_empty = self
                            .writer
                            .get_children_ensure(conflict_node_id)?
                            .next()
                            .is_none();
                        let new_is_empty = self
                            .writer
                            .get_children(child_id)
                            .map(|mut n| n.next().is_none())
                            .unwrap_or(true);
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
                                .writer
                                .get_refs(conflict_node_id)
                                .map(|r| r.map(|r| r.borrow().clone()).collect::<Vec<_>>())
                                .unwrap_or_default();

                            for r in refs {
                                dos.push(Do::Ref(r, Some(child_id)));
                            }

                            dos.push(Do::Move {
                                id: conflict_node_id,
                                to: Some((
                                    CONFLICT,
                                    TrieKey(conflict_node_id.to_string()),
                                    conflict_node.borrow().content.to_owned(),
                                )),
                            });

                            dos.push(Do::Move {
                                id: child_id,
                                to: Some((
                                    parent_id,
                                    op.child_key.to_owned(),
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
                        op.child_key.to_owned(),
                        op.child_content.to_owned(),
                    )),
                });
            }
        }

        let mut undos = Vec::with_capacity(dos.len());

        for d in dos {
            undos.push(self.exec_do(d)?)
        }

        Ok(LogOp { op, undos })
    }

    fn exec_do(&mut self, d: Do<C>) -> Result<Undo<C>> {
        Ok(match d {
            Do::Ref(r, id) => {
                let old_id = self.writer.set_ref(r.to_owned(), id)?;
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
                self.writer.set_ref(r, id)?;
            }
            Undo::Move { id, to } => {
                self.move_node(id, to)?;
            }
        })
    }

    fn undo_op(&mut self, log: &LogOp<M, C>) -> Result<()> {
        for undo in log.undos.iter().rev().cloned() {
            self.exec_undo(undo)?
        }

        Ok(())
    }

    fn redo_op(&mut self, log: &LogOp<M, C>) -> Result<LogOp<M, C>> {
        self.do_op(log.op.clone())
    }

    pub fn apply(&mut self, ops: Vec<Op<M, C>>) -> Result<&mut Self> {
        let mut redo_queue = Vec::new();
        if let Some(first_op) = ops.first() {
            loop {
                if let Some(last) = self.writer.pop_log()? {
                    match first_op.marker.partial_cmp(&last.op.marker) {
                        None | Some(Ordering::Equal) => {
                            return Err(Error::InvalidOp(
                                "The marker of the operation has duplicates. Every op must have a unique timestamp.".to_string(),
                            ));
                        }
                        Some(Ordering::Less) => {
                            self.undo_op(&last)?;
                            redo_queue.push(last)
                        }
                        Some(Ordering::Greater) => {
                            self.writer.push_log(last)?;
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
                if let Some(redo) = redo_queue.pop() {
                    match op.marker.partial_cmp(&redo.op.marker) {
                        None | Some(Ordering::Equal) => {
                            return Err(Error::InvalidOp(
                              "The marker of the operation has duplicates. Every op must have a unique timestamp.".to_string(),
                          ));
                        }
                        Some(Ordering::Less) => {
                            let log_op = self.do_op(op)?;
                            self.writer.push_log(log_op)?;
                            redo_queue.push(redo);
                            break;
                        }
                        Some(Ordering::Greater) => {
                            let redo_log_op = self.redo_op(&redo)?;
                            self.writer.push_log(redo_log_op)?;
                            break;
                        }
                    }
                } else {
                    let log_op = self.do_op(op)?;
                    self.writer.push_log(log_op)?;
                    break;
                }
            }
        }
        for redo in redo_queue.into_iter().rev() {
            self.redo_op(&redo)?;
            self.writer.push_log(redo)?;
        }

        self.calculate_hash(ROOT)?;

        Ok(self)
    }

    pub fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
