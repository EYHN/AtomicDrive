pub mod backend;

use std::{borrow::Borrow, cmp::Ordering, fmt::Display, marker::PhantomData};

use backend::{TrieBackend, TrieBackendWriter};
use std::fmt::Debug;
use thiserror::Error;
use utils::{tree_stringify, Deserialize, Digestible, Serialize};
use uuid::Uuid;

use std::hash::Hash;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Operation may break the tree, or the tree is already broken. {0}")]
    TreeBroken(String),
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
    #[error("Decode error, {0}")]
    DecodeError(String),
    #[error("rocksdb error")]
    RocksdbError(#[from] rocksdb::Error),
    #[error("db error")]
    DBError(#[from] db::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub enum TrieDiff {
    Moved(Option<TrieId>, Option<TrieId>),
}

pub trait TrieContent: Clone + Hash + Default + Digestible {}
impl<T: Clone + Hash + Default + Digestible> TrieContent for T {}

pub trait TrieMarker: PartialOrd + Clone + Hash {}
impl<A: PartialOrd + Clone + Hash> TrieMarker for A {}

pub const ROOT: TrieId = TrieId(0u64.to_be_bytes());
pub const CONFLICT: TrieId = TrieId(1u64.to_be_bytes());

/// Tree node id
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TrieId(pub [u8; 8]);

impl Display for TrieId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&u64::from_be_bytes(self.0), f)
    }
}

impl TrieId {
    pub fn inc(&self) -> Self {
        TrieId((u64::from_be_bytes(self.0) + 1).to_be_bytes())
    }

    pub fn id(&self) -> u64 {
        u64::from_be_bytes(self.0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for TrieId {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8> {
        self.0.serialize(bytes)
    }
}

impl Deserialize for TrieId {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (id, rest) = <[u8; 8]>::deserialize(bytes)?;
        Ok((TrieId(id), rest))
    }
}

impl From<u64> for TrieId {
    fn from(value: u64) -> Self {
        Self(value.to_be_bytes())
    }
}

/// The key of the tree
#[derive(Debug, Default, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TrieKey(pub String);

impl TrieKey {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Serialize for TrieKey {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8> {
        self.0.serialize(bytes)
    }
}

impl Deserialize for TrieKey {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (key, rest) = String::deserialize(bytes)?;
        Ok((TrieKey(key), rest))
    }
}

impl From<String> for TrieKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Display for TrieKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// The reference of the node, which is used to determine the node of the operation during the distributed operation
#[derive(Debug, Default, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct TrieRef(pub [u8; 16]);

impl TrieRef {
    pub fn new() -> Self {
        TrieRef(Uuid::new_v4().to_u128_le().to_be_bytes())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for TrieRef {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8> {
        self.0.serialize(bytes)
    }
}

impl Deserialize for TrieRef {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (r, rest) = <_>::deserialize(bytes)?;
        Ok((TrieRef(r), rest))
    }
}

impl From<u128> for TrieRef {
    fn from(value: u128) -> Self {
        Self(value.to_be_bytes())
    }
}

impl Display for TrieRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&Uuid::from_bytes_ref(&self.0), f)
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

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for TrieHash {
    fn serialize(&self, bytes: Vec<u8>) -> Vec<u8> {
        self.0.serialize(bytes)
    }
}

impl Deserialize for TrieHash {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (r, rest) = <_>::deserialize(bytes)?;
        Ok((TrieHash(r), rest))
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
pub struct TrieNode<C: TrieContent> {
    pub parent: TrieId,
    pub key: TrieKey,
    pub content: C,
}

impl<C: TrieContent + Serialize> Serialize for TrieNode<C> {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes = self.parent.serialize(bytes);
        bytes = self.key.serialize(bytes);
        bytes = self.content.serialize(bytes);
        bytes
    }
}

impl<C: TrieContent + Deserialize> Deserialize for TrieNode<C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (parent, bytes) = TrieId::deserialize(bytes)?;
        let (key, bytes) = TrieKey::deserialize(bytes)?;
        let (content, bytes) = C::deserialize(bytes)?;

        Ok((
            Self {
                parent,
                key,
                content,
            },
            bytes,
        ))
    }
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

impl<C: TrieContent + Serialize> Serialize for Undo<C> {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        match self {
            Undo::Ref(r, id) => {
                bytes.push(b'r');
                bytes = r.serialize(bytes);
                if let Some(id) = id {
                    bytes.push(b'i');
                    bytes = id.serialize(bytes);
                } else {
                    bytes.push(b'n');
                }
                bytes
            }
            Undo::Move { id, to } => {
                bytes.push(b'm');
                bytes = id.serialize(bytes);
                if let Some((to_id, to_key, to_c)) = to {
                    bytes.push(b'i');
                    bytes = to_id.serialize(bytes);
                    bytes = to_key.serialize(bytes);
                    bytes = to_c.serialize(bytes);
                } else {
                    bytes.push(b'n');
                }
                bytes
            }
        }
    }
}

impl<C: TrieContent + Deserialize> Deserialize for Undo<C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        match bytes[0] {
            b'r' => {
                let (r, bytes) = TrieRef::deserialize(&bytes[1..])?;
                let (id, bytes) = if bytes[0] == b'i' {
                    let (id, bytes) = TrieId::deserialize(bytes)?;
                    (Some(id), bytes)
                } else {
                    (None, &bytes[1..])
                };
                Ok((Undo::Ref(r, id), bytes))
            }
            b'm' => {
                let (id, bytes) = TrieId::deserialize(&bytes[1..])?;
                let (to, bytes) = if bytes[0] == b'i' {
                    let (to_id, bytes) = TrieId::deserialize(bytes)?;
                    let (to_key, bytes) = TrieKey::deserialize(bytes)?;
                    let (to_c, bytes) = C::deserialize(bytes)?;
                    (Some((to_id, to_key, to_c)), bytes)
                } else {
                    (None, &bytes[1..])
                };
                Ok((Undo::Move { id, to }, bytes))
            }
            _ => Err(format!("Failed to decode undo: {bytes:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op<M: TrieMarker, C: TrieContent> {
    pub marker: M,
    pub parent_ref: TrieRef,
    pub child_key: TrieKey,
    pub child_ref: TrieRef,
    pub child_content: C,
}

impl<M: TrieMarker + Serialize, C: TrieContent + Serialize> Serialize for Op<M, C> {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes = self.parent_ref.serialize(bytes);
        bytes = self.child_key.serialize(bytes);
        bytes = self.child_ref.serialize(bytes);
        bytes = self.marker.serialize(bytes);
        bytes = self.child_content.serialize(bytes);
        bytes
    }
}

impl<M: TrieMarker + Deserialize, C: TrieContent + Deserialize> Deserialize for Op<M, C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (parent_ref, bytes) = TrieRef::deserialize(bytes)?;
        let (child_key, bytes) = TrieKey::deserialize(bytes)?;
        let (child_ref, bytes) = TrieRef::deserialize(bytes)?;
        let (marker, bytes) = M::deserialize(bytes)?;
        let (child_content, bytes) = C::deserialize(bytes)?;

        Ok((
            Self {
                marker,
                parent_ref,
                child_key,
                child_ref,
                child_content,
            },
            bytes,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogOp<M: TrieMarker, C: TrieContent> {
    pub op: Op<M, C>,
    pub undos: Vec<Undo<C>>,
}

impl<M: TrieMarker + Serialize, C: TrieContent + Serialize> Serialize for LogOp<M, C> {
    fn serialize(&self, mut bytes: Vec<u8>) -> Vec<u8> {
        bytes = self.op.serialize(bytes);
        bytes = self.undos.serialize(bytes);
        bytes
    }
}

impl<M: TrieMarker + Deserialize, C: TrieContent + Deserialize> Deserialize for LogOp<M, C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (op, bytes) = Op::<M, C>::deserialize(bytes)?;
        let (undos, bytes) = Vec::<Undo<C>>::deserialize(bytes)?;

        Ok((Self { op, undos }, bytes))
    }
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
        self.dbg_itemization(ROOT, "", &mut items);

        f.write_str(&tree_stringify(
            items.iter().map(|(path, _, node)| {
                (
                    path.as_ref(),
                    if node.content.to_string().is_empty() {
                        "".to_string()
                    } else {
                        format!("[{}]", node.content)
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
        self.dbg_itemization(ROOT, "", &mut items);

        f.write_str(&tree_stringify(
            items
                .iter()
                .map(|(path, id, node)| (path.as_ref(), format!("[{}] #{}", node.content, id))),
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
        let children = self.backend.get_children(root).unwrap();
        let path = format!("{}/{}", prefix, node.borrow().key);
        base.push((path.clone(), root, node.borrow().clone()));

        for child in children {
            let (_, id) = child.unwrap();
            self.dbg_itemization(*id.borrow(), &path, base)
        }
    }

    #[allow(dead_code)]
    fn diff<OtherB: TrieBackend<M, C>>(&self, _other: &Trie<M, C, OtherB>) -> Vec<TrieDiff> {
        todo!()
    }
}

impl<M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> std::ops::Deref for Trie<M, C, B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

pub struct TrieUpdater<'a, M: TrieMarker, C: TrieContent, B: TrieBackend<M, C> + 'a> {
    writer: B::Writer<'a>,
}

impl<M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> TrieUpdater<'_, M, C, B> {
    fn move_node(
        &mut self,
        id: TrieId,
        to: Option<(TrieId, TrieKey, C)>,
    ) -> Result<Option<(TrieId, TrieKey, C)>> {
        let old = self.writer.set_tree_node(id, to)?;
        Ok(old)
    }

    fn do_op(&mut self, op: Op<M, C>) -> Result<LogOp<M, C>> {
        let mut dos: Vec<Do<C>> = Vec::with_capacity(3);
        let child_id = if let Some(child_id) = self.writer.get_id(op.child_ref.to_owned())? {
            child_id.to_owned()
        } else {
            let new_id = self.writer.create_id()?;
            dos.push(Do::Ref(op.child_ref.to_owned(), Some(new_id)));
            new_id
        };
        let parent_id = if let Some(parent_id) = self.writer.get_id(op.parent_ref.to_owned())? {
            parent_id.to_owned()
        } else {
            return Err(Error::TreeBroken(format!(
                "parent ref {:?} not found",
                &op.parent_ref
            )));
        };

        // ensures no cycles are introduced.
        'c: {
            if child_id != parent_id && !self.writer.is_ancestor(parent_id, child_id)? {
                if let Some(conflict_node_id) =
                    self.writer.get_child(parent_id, op.child_key.to_owned())?
                {
                    if conflict_node_id != child_id {
                        let conflict_node = self.writer.get_ensure(conflict_node_id)?;
                        let conflict_is_empty =
                            self.writer.get_children(conflict_node_id)?.next().is_none();
                        let new_is_empty = self.writer.get_children(child_id)?.next().is_none();
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
                            if let Some(refs) = self.writer.get_refs(conflict_node_id)? {
                                for r in refs {
                                    dos.push(Do::Ref(r.borrow().clone(), Some(child_id)));
                                }
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
        match d {
            Undo::Ref(r, id) => {
                self.writer.set_ref(r, id)?;
            }
            Undo::Move { id, to } => {
                self.move_node(id, to)?;
            }
        };
        Ok(())
    }

    fn undo_op(&mut self, log: LogOp<M, C>) -> Result<Op<M, C>> {
        for undo in log.undos.iter().rev().cloned() {
            self.exec_undo(undo)?
        }

        Ok(log.op)
    }

    pub fn apply(&mut self, ops: Vec<Op<M, C>>) -> Result<&mut Self> {
        let mut redo_queue = Vec::new();
        if let Some(first_op) = ops.first() {
            while let Some(last) = self.writer.pop_log()? {
                match first_op.marker.partial_cmp(&last.op.marker) {
                    None | Some(Ordering::Equal) => {
                        return Err(Error::InvalidOp(
                                "The marker of the operation has duplicates. Every op must have a unique timestamp.".to_string(),
                            ));
                    }
                    Some(Ordering::Less) => {
                        redo_queue.push(self.undo_op(last)?);
                    }
                    Some(Ordering::Greater) => {
                        self.writer.push_log(last)?;
                        break;
                    }
                }
            }
        }

        for op in ops {
            loop {
                if let Some(redo) = redo_queue.pop() {
                    match op.marker.partial_cmp(&redo.marker) {
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
                            let redo_log_op: LogOp<M, C> = self.do_op(redo)?;
                            self.writer.push_log(redo_log_op)?;
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
            let redo_log_op: LogOp<M, C> = self.do_op(redo)?;
            self.writer.push_log(redo_log_op)?;
        }

        Ok(self)
    }

    pub fn commit(self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }
}

impl<'a, M: TrieMarker, C: TrieContent, B: TrieBackend<M, C>> std::ops::Deref
    for TrieUpdater<'a, M, C, B>
{
    type Target = B::Writer<'a>;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

#[cfg(test)]
mod tests;
