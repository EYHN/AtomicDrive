pub mod store;

use std::{borrow::Borrow, cmp::Ordering, fmt::Display, marker::PhantomData};

use db::{DBLock, DBRead, DBTransaction, DBWrite, DB};
use std::fmt::Debug;
use store::{TrieStore, TrieStoreWriter};
use thiserror::Error;
use utils::{tree_stringify, Deserialize, Digestible, Serialize, Serializer};
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
    #[error("db error")]
    DBError(#[from] db::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub enum TrieDiff {
    Moved(Option<TrieId>, Option<TrieId>),
}

pub trait TrieContent: Clone + Hash + Default + Digestible + Serialize + Deserialize {}
impl<T: Clone + Hash + Default + Digestible + Serialize + Deserialize> TrieContent for T {}

pub trait TrieMarker: PartialOrd + Clone + Hash + Serialize + Deserialize {}
impl<A: PartialOrd + Clone + Hash + Serialize + Deserialize> TrieMarker for A {}

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
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        self.0.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        self.0.byte_size()
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
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        self.0.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        self.0.byte_size()
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
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        self.0.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        self.0.byte_size()
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
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        self.0.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        self.0.byte_size()
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

impl<C: TrieContent> Serialize for TrieNode<C> {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer = self.parent.serialize(serializer);
        serializer = self.key.serialize(serializer);
        serializer = self.content.serialize(serializer);
        serializer
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.parent.byte_size()? + self.key.byte_size()? + self.content.byte_size()?)
    }
}

impl<C: TrieContent> Deserialize for TrieNode<C> {
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

impl<C: TrieContent> Serialize for Undo<C> {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        match self {
            Undo::Ref(r, id) => {
                serializer.push(b'r');
                serializer = r.serialize(serializer);
                if let Some(id) = id {
                    serializer.push(b'i');
                    serializer = id.serialize(serializer);
                } else {
                    serializer.push(b'n');
                }
                serializer
            }
            Undo::Move { id, to } => {
                serializer.push(b'm');
                serializer = id.serialize(serializer);
                if let Some((to_id, to_key, to_c)) = to {
                    serializer.push(b'i');
                    serializer = to_id.serialize(serializer);
                    serializer = to_key.serialize(serializer);
                    serializer = to_c.serialize(serializer);
                } else {
                    serializer.push(b'n');
                }
                serializer
            }
        }
    }

    fn byte_size(&self) -> Option<usize> {
        Some(match self {
            Undo::Ref(r, id) => {
                if let Some(id) = id {
                    1 + r.byte_size()? + 1 + id.byte_size()?
                } else {
                    1 + r.byte_size()? + 1
                }
            }
            Undo::Move { id, to } => {
                if let Some((to_id, to_key, to_c)) = to {
                    1 + id.byte_size()?
                        + 1
                        + to_id.byte_size()?
                        + to_key.byte_size()?
                        + to_c.byte_size()?
                } else {
                    1 + id.byte_size()? + 1
                }
            }
        })
    }
}

impl<C: TrieContent> Deserialize for Undo<C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        match bytes[0] {
            b'r' => {
                let (r, bytes) = TrieRef::deserialize(&bytes[1..])?;
                let (id, bytes) = if bytes[0] == b'i' {
                    let (id, bytes) = TrieId::deserialize(&bytes[1..])?;
                    (Some(id), bytes)
                } else {
                    (None, &bytes[1..])
                };
                Ok((Undo::Ref(r, id), bytes))
            }
            b'm' => {
                let (id, bytes) = TrieId::deserialize(&bytes[1..])?;
                let (to, bytes) = if bytes[0] == b'i' {
                    let (to_id, bytes) = TrieId::deserialize(&bytes[1..])?;
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

impl<M: TrieMarker, C: TrieContent> Serialize for Op<M, C> {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer = self.marker.serialize(serializer);
        serializer = self.parent_ref.serialize(serializer);
        serializer = self.child_key.serialize(serializer);
        serializer = self.child_ref.serialize(serializer);
        serializer = self.child_content.serialize(serializer);
        serializer
    }

    fn byte_size(&self) -> Option<usize> {
        Some(
            self.marker.byte_size()?
                + self.parent_ref.byte_size()?
                + self.child_key.byte_size()?
                + self.child_ref.byte_size()?
                + self.child_content.byte_size()?,
        )
    }
}

impl<M: TrieMarker, C: TrieContent> Deserialize for Op<M, C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (marker, bytes) = M::deserialize(bytes)?;
        let (parent_ref, bytes) = TrieRef::deserialize(bytes)?;
        let (child_key, bytes) = TrieKey::deserialize(bytes)?;
        let (child_ref, bytes) = TrieRef::deserialize(bytes)?;
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

impl<M: TrieMarker, C: TrieContent> Serialize for LogOp<M, C> {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer = self.op.serialize(serializer);
        serializer = self.undos.serialize(serializer);
        serializer
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.op.byte_size()? + self.undos.byte_size()?)
    }
}

impl<M: TrieMarker, C: TrieContent> Deserialize for LogOp<M, C> {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (op, bytes) = Op::<M, C>::deserialize(bytes)?;
        let (undos, bytes) = Vec::<Undo<C>>::deserialize(bytes)?;

        Ok((Self { op, undos }, bytes))
    }
}

#[derive(Clone)]
pub struct Trie<M: TrieMarker, C: TrieContent, DBImpl: DB> {
    store: TrieStore<DBImpl, M, C>,
    m: PhantomData<M>,
    c: PhantomData<C>,
}

impl<M: TrieMarker, C: TrieContent + Display, DBImpl: DB> Display for Trie<M, C, DBImpl> {
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

impl<M: TrieMarker, C: TrieContent + Display, DBImpl: DB> Debug for Trie<M, C, DBImpl> {
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

impl<M: TrieMarker, C: TrieContent, DBImpl: DB> Trie<M, C, DBImpl> {
    pub fn init(db: DBImpl) -> Result<Self> {
        Ok(Trie {
            store: TrieStore::init(db)?,
            m: Default::default(),
            c: Default::default(),
        })
    }

    pub fn from_db(db: DBImpl) -> Self {
        Trie {
            store: TrieStore::from_db(db),
            m: Default::default(),
            c: Default::default(),
        }
    }

    pub fn write(&mut self) -> Result<TrieUpdater<M, C, DBImpl::Transaction<'_>>> {
        Ok(TrieUpdater {
            writer: self.store.write()?,
        })
    }

    fn dbg_itemization(
        &self,
        root: TrieId,
        prefix: &str,
        base: &mut Vec<(String, TrieId, TrieNode<C>)>,
    ) {
        let node = self.store.get_ensure(root).unwrap();
        let children = self.store.get_children(root).unwrap();
        let path = format!("{}/{}", prefix, node.borrow().key);
        base.push((path.clone(), root, node.borrow().clone()));

        for child in children {
            let (_, id) = child.unwrap();
            self.dbg_itemization(*id.borrow(), &path, base)
        }
    }
}

impl<M: TrieMarker, C: TrieContent, DBImpl: DB> std::ops::Deref for Trie<M, C, DBImpl> {
    type Target = TrieStore<DBImpl, M, C>;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

pub struct TrieUpdater<M: TrieMarker, C: TrieContent, DBImpl> {
    writer: TrieStoreWriter<DBImpl, M, C>,
}

impl<M: TrieMarker, C: TrieContent, DBImpl: DBRead + DBWrite + DBLock> TrieUpdater<M, C, DBImpl> {
    pub fn from_db(db: DBImpl) -> Self {
        TrieUpdater {
            writer: TrieStoreWriter::from_db(db),
        }
    }

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
}

impl<M: TrieMarker, C: TrieContent, DBImpl: DBTransaction> TrieUpdater<M, C, DBImpl> {
    pub fn commit(self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.writer.rollback()?;
        Ok(())
    }
}

impl<M: TrieMarker, C: TrieContent, DBImpl: DBTransaction> std::ops::Deref
    for TrieUpdater<M, C, DBImpl>
{
    type Target = TrieStoreWriter<DBImpl, M, C>;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

#[cfg(test)]
mod tests;