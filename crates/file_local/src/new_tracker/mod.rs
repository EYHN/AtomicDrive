//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

use std::{backtrace::Backtrace, fmt::Display};

use db::{DBLock, DBRead, DBTransaction, DBWrite, DB};
use file::{FileFullPath, FileType};
use thiserror::Error;
use trie::trie::{
    Error as TrieError, Op, OpTarget, Trie, TrieId, TrieKey, TrieRef, TrieTransaction, RECYCLE,
    RECYCLE_REF, ROOT,
};
use utils::{bytes_stringify, Deserialize, Digest, Digestible, PathTools, Serialize};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
    #[error("Decode error, {0}")]
    DecodeError(String, Backtrace),
    #[error("Trie error")]
    TrieError(#[from] TrieError),
    #[error("db error")]
    DBError(#[from] db::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// store file system level file identifier, e.g. inode number in linux, file_id in windows
/// https://man7.org/linux/man-pages/man7/inode.7.html
/// https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info
///
/// Sometimes reliable, sometimes not
type FileMarker = Vec<u8>;

type FileName = String;

/// Store information about whether the file is updated, Usually is a combination of file mtime and size.
type FileUpdateMarker = Vec<u8>;

/// the file IDs we managed, is reliable.
type FileId = u128;

/// Since we will never conflict, use a simple u128 as the clock
type Clock = u128;

#[derive(Debug, Clone, Default)]
pub struct Entity {
    file_id: FileId,
    marker: FileMarker,
    update_marker: FileUpdateMarker,
    is_directory: bool,
}

impl Entity {
    fn empty_directory(file_id: FileId) -> Self {
        Self {
            file_id,
            is_directory: true,
            ..Default::default()
        }
    }

    fn file(file_id: FileId, marker: FileMarker, update_marker: FileUpdateMarker) -> Self {
        Self {
            file_id,
            marker,
            update_marker,
            is_directory: true,
        }
    }
}

impl Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.file_id.to_string())?;
        if self.is_directory {
            f.write_str(" dir")?;
        } else {
            f.write_str(" file")?;
        }
        Ok(())
    }
}

impl Serialize for Entity {
    fn serialize(&self, serializer: utils::Serializer) -> utils::Serializer {
        let serializer = self.file_id.serialize(serializer);
        let serializer = self.marker.serialize(serializer);
        let serializer = self.update_marker.serialize(serializer);
        self.is_directory.serialize(serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(
            self.file_id.byte_size()?
                + self.marker.byte_size()?
                + self.update_marker.byte_size()?
                + self.is_directory.byte_size()?,
        )
    }
}

impl Deserialize for Entity {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (file_id, bytes) = <_>::deserialize(bytes)?;
        let (marker, bytes) = <_>::deserialize(bytes)?;
        let (update_marker, bytes) = <_>::deserialize(bytes)?;
        let (is_directory, bytes) = <_>::deserialize(bytes)?;

        Ok((
            Self {
                file_id,
                marker,
                update_marker,
                is_directory,
            },
            bytes,
        ))
    }
}

impl Digestible for Entity {
    fn digest(&self, data: &mut impl Digest) {
        self.file_id.digest(data);
        self.marker.digest(data);
        self.update_marker.digest(data);
        self.is_directory.digest(data)
    }
}

#[derive(Debug)]
pub enum IndexInput {
    File(FileFullPath, FileType, FileMarker, FileUpdateMarker),
    Directory(
        FileFullPath,
        FileMarker,
        FileUpdateMarker,
        Vec<(FileName, FileType, FileMarker, FileUpdateMarker)>,
    ),
    Empty(FileFullPath),
}

const DB_TRIE_PREFIX: &[u8] = b"trie:";
const CLOCK_KEY: &[u8] = b"current_clock";
const AUTO_INCREMENT_FILE_ID_KEY: &[u8] = b"auto_increment_file_id";
const MARKERS_PREFIX: &[u8] = b"mk:";

pub struct Tracker<DBImpl: DB> {
    db: DBImpl,
}

impl<DBImpl: DB> Tracker<DBImpl> {
    fn init(db: DBImpl) -> Result<Self> {
        Trie::<u128, Entity, _>::init(db::DB::prefix(&db, DB_TRIE_PREFIX))?;
        let mut transaction = db.start_transaction()?;
        if !transaction.has(AUTO_INCREMENT_FILE_ID_KEY)? {
            transaction.set(AUTO_INCREMENT_FILE_ID_KEY, 0u128.to_bytes())?;
        }
        if !transaction.has(CLOCK_KEY)? {
            transaction.set(CLOCK_KEY, 0u128.to_bytes())?;
        }
        transaction.commit()?;
        Ok(Tracker { db })
    }

    fn trie(&self) -> Trie<u128, Entity, db::prefix::Prefix<&'_ DBImpl>> {
        Trie::from_db(db::DB::prefix(&self.db, DB_TRIE_PREFIX))
    }

    fn start_transaction(&self) -> db::Result<TrackerTransaction<DBImpl::Transaction<'_>>> {
        Ok(TrackerTransaction {
            db: self.db.start_transaction()?,
            current_ops: Default::default(),
        })
    }
}

pub struct TrackerTransaction<DBImpl: DBRead + DBWrite + DBLock> {
    db: DBImpl,
    current_ops: Vec<Op<u128, Entity>>,
}

impl<DBImpl: DBRead + DBWrite + DBLock> TrackerTransaction<DBImpl> {
    fn lock(&mut self) -> Result<()> {
        self.auto_increment_clock()?;
        self.trie().lock()?;

        Ok(())
    }

    fn trie(&mut self) -> TrieTransaction<u128, Entity, db::prefix::Prefix<&'_ mut DBImpl>> {
        TrieTransaction::from_db(db::prefix::Prefix::new(&mut self.db, DB_TRIE_PREFIX))
    }

    fn marker_get(&self, file_marker: &FileMarker) -> Result<Option<TrieId>> {
        let mut key = Vec::with_capacity(MARKERS_PREFIX.len() + file_marker.len());
        key.extend_from_slice(MARKERS_PREFIX);
        key.extend_from_slice(file_marker);
        self.db
            .get(key)?
            .map(|d| TrieId::from_bytes(d.as_ref()))
            .transpose()
            .map_err(|err| Error::DecodeError(err, Backtrace::capture()))
    }

    fn marker_set(&mut self, file_marker: &FileMarker, file_id: &TrieId) -> Result<()> {
        let mut key = Vec::with_capacity(MARKERS_PREFIX.len() + file_marker.len());
        key.extend_from_slice(MARKERS_PREFIX);
        key.extend_from_slice(file_marker);
        self.db.set(key, file_id.as_bytes())?;

        Ok(())
    }

    fn auto_increment_file_id(&mut self) -> Result<FileId> {
        let old_file_id = {
            let bytes = self
                .db
                .get(AUTO_INCREMENT_FILE_ID_KEY)?
                .ok_or(Error::InvalidOp(
                    "Tracker Database not initialized.".to_owned(),
                ))?;

            u128::from_be_bytes(bytes.as_ref().try_into().map_err(|_| {
                Error::DecodeError("failed to decode file id".to_string(), Backtrace::capture())
            })?)
        };

        let new_file_id = old_file_id + 1;

        self.db
            .set(AUTO_INCREMENT_FILE_ID_KEY, new_file_id.to_be_bytes())?;

        Ok(new_file_id)
    }

    fn auto_increment_clock(&mut self) -> Result<Clock> {
        let clock = {
            let bytes = self.db.get_for_update(CLOCK_KEY)?.ok_or(Error::InvalidOp(
                "Tracker Database not initialized.".to_owned(),
            ))?;
            Clock::from_bytes(bytes.as_ref())
                .map_err(|err| Error::DecodeError(err, Backtrace::capture()))?
                + 1
        };

        self.db.set(CLOCK_KEY, &clock.to_bytes())?;

        Ok(clock)
    }

    fn move_node_to_recycle(&mut self, node: TrieId) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.apply(Op {
            marker: new_clock,
            parent_target: RECYCLE.into(),
            child_key: TrieKey(node.id().to_string()),
            child_target: node.into(),
            child_content: None,
        })
    }

    fn create_untracked_directory(&mut self, parent: TrieId, key: TrieKey) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;
        let directory = Entity::empty_directory(self.auto_increment_file_id()?);
        self.apply(Op {
            marker: new_clock,
            parent_target: parent.into(),
            child_key: key,
            child_target: OpTarget::NewId,
            child_content: Some(directory),
        })
    }

    fn create_node(
        &mut self,
        to: TrieId,
        key: TrieKey,
        marker: FileMarker,
        update_marker: FileUpdateMarker,
        is_directory: bool,
    ) -> Result<TrieId> {
        let new_clock = self.auto_increment_clock()?;
        let new_id = self.trie().create_id()?;

        self.marker_set(&marker, &new_id)?;

        let new_node = Entity {
            file_id: self.auto_increment_file_id()?,
            is_directory,
            marker,
            update_marker,
        };
        self.apply(Op {
            marker: new_clock,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(new_id),
            child_content: Some(new_node),
        })?;

        Ok(new_id)
    }

    fn move_and_update_node(
        &mut self,
        to: TrieId,
        key: TrieKey,
        node: TrieId,
        old_content: &Entity,
        update_marker: FileUpdateMarker,
    ) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.apply(Op {
            marker: new_clock,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(node),
            child_content: Some(Entity {
                file_id: old_content.file_id.clone(),
                is_directory: old_content.is_directory,
                marker: old_content.marker.clone(),
                update_marker,
            }),
        })
    }

    fn mark_node_as_untracked(
        &mut self,
        to: TrieId,
        key: TrieKey,
        node: TrieId,
        node_content: Entity,
    ) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.apply(Op {
            marker: new_clock,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(node),
            child_content: Some(Entity {
                file_id: self.auto_increment_file_id()?,
                is_directory: node_content.is_directory,
                marker: Default::default(),
                update_marker: Default::default(),
            }),
        })
    }

    fn update_node_update_marker(
        &mut self,
        to: TrieId,
        key: TrieKey,
        id: TrieId,
        old_content: &Entity,
        update_marker: FileUpdateMarker,
    ) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;
        self.apply(Op {
            marker: new_clock,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(id),
            child_content: Some(Entity {
                file_id: old_content.file_id.clone(),
                is_directory: old_content.is_directory,
                marker: old_content.marker.clone(),
                update_marker,
            }),
        })
    }

    fn update_untrack_node(
        &mut self,
        to: TrieId,
        key: TrieKey,
        id: TrieId,
        old_content: &Entity,
        marker: FileUpdateMarker,
        update_marker: FileUpdateMarker,
    ) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.marker_set(&marker, &id)?;

        self.apply(Op {
            marker: new_clock,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(id),
            child_content: Some(Entity {
                file_id: old_content.file_id,
                is_directory: old_content.is_directory,
                marker,
                update_marker,
            }),
        })
    }

    fn create_or_move_node(
        &mut self,
        to: TrieId,
        key: TrieKey,
        marker: FileMarker,
        update_marker: FileUpdateMarker,
        is_directory: bool,
    ) -> Result<TrieId> {
        let old_child_id = self.trie().get_child(to, key.clone())?;
        if let Some(old_child_id) = old_child_id {
            let old_child = self.trie().get_ensure(old_child_id)?;
            if old_child.content.is_directory != is_directory {
                self.move_node_to_recycle(old_child_id)?;
            } else if old_child.content.marker.is_empty() {
                if self.marker_get(&marker)?.is_some() {
                    self.move_node_to_recycle(old_child_id)?;
                } else {
                    self.update_untrack_node(
                        to,
                        key,
                        old_child_id,
                        &old_child.content,
                        marker,
                        update_marker,
                    )?;
                    return Ok(old_child_id);
                }
            } else if old_child.content.marker != marker {
                self.move_node_to_recycle(old_child_id)?;
            } else if old_child.content.update_marker != update_marker {
                self.update_node_update_marker(
                    to,
                    key,
                    old_child_id,
                    &old_child.content,
                    update_marker,
                )?;
                return Ok(old_child_id);
            } else {
                return Ok(old_child_id);
            }
        }

        if let Some(old_id) = self.marker_get(&marker)? {
            let old_file = self.trie().get_ensure(old_id)?;
            if !self.trie().is_ancestor(to, old_id)? {
                self.move_and_update_node(to, key, old_id, &old_file.content, update_marker)?;
                Ok(old_id)
            } else {
                self.mark_node_as_untracked(
                    old_file.parent,
                    old_file.key,
                    old_id,
                    old_file.content,
                )?;
                self.create_node(to, key, marker, update_marker, is_directory)
            }
        } else {
            self.create_node(to, key, marker, update_marker, is_directory)
        }
    }

    fn apply(&mut self, op: Op<u128, Entity>) -> Result<()> {
        self.trie().apply(vec![op.clone()])?;
        self.current_ops.push(op);
        Ok(())
    }

    pub fn index(&mut self, input: IndexInput) -> Result<Vec<Op<u128, Entity>>> {
        self.auto_increment_clock()?; // global lock
        self.current_ops = vec![];
        if let IndexInput::Empty(full_path) = input {
            // delete the file in the full path
            if let Some(old_file_id) = self.trie().get_id_by_path(full_path.as_ref())? {
                self.move_node_to_recycle(old_file_id)?;
                return Ok(core::mem::take(&mut self.current_ops));
            }

            Ok(vec![])
        } else {
            let full_path = match &input {
                IndexInput::File(full_path, _, _, _) => full_path,
                IndexInput::Directory(full_path, _, _, _) => full_path,
                IndexInput::Empty(_) => unreachable!(),
            };

            // step 1: create parent directory if not exists
            let parent_full_path = full_path.dirname();
            let mut parent_id = ROOT; // start from root
            for part in PathTools::parts(parent_full_path.as_ref()) {
                if let Some(node_id) = self.trie().get_child(parent_id, TrieKey(part.to_owned()))? {
                    // if exists, check the node is directory
                    let child = self.trie().get_ensure(node_id)?;
                    if !child.content.is_directory {
                        // if not directory, move the node to recycle, and create a directory
                        self.move_node_to_recycle(node_id)?;
                        self.create_untracked_directory(parent_id, part.to_owned().into())?;
                        parent_id = self
                            .trie()
                            .get_child_ensure(parent_id, TrieKey(part.to_owned()))?;
                    } else {
                        parent_id = node_id
                    }
                } else {
                    // if not exists, create a directory
                    self.create_untracked_directory(parent_id, part.to_owned().into())?;
                    parent_id = self
                        .trie()
                        .get_child_ensure(parent_id, TrieKey(part.to_owned()))?;
                }
            }

            if let IndexInput::File(full_path, _, file_marker, file_update_marker) = input {
                let file_name = PathTools::basename(full_path.as_ref());
                self.create_or_move_node(
                    parent_id,
                    file_name.to_owned().into(),
                    file_marker,
                    file_update_marker,
                    false,
                )?;
            } else if let IndexInput::Directory(
                full_path,
                dir_marker,
                dir_update_marker,
                children,
            ) = input
            {
                let file_name = PathTools::basename(full_path.as_ref());
                let parent_id = self.create_or_move_node(
                    parent_id,
                    file_name.to_owned().into(),
                    dir_marker,
                    dir_update_marker,
                    true,
                )?;
                for (file_name, file_type, marker, update_marker) in children {
                    self.create_or_move_node(
                        parent_id,
                        file_name.to_owned().into(),
                        marker,
                        update_marker,
                        file_type == FileType::Directory,
                    )?;
                }
            } else {
                unreachable!()
            }

            Ok(core::mem::take(&mut self.current_ops))
        }
    }
}

impl<DBImpl: DBTransaction> TrackerTransaction<DBImpl> {
    pub fn commit(self) -> Result<()> {
        self.db.commit()?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.db.rollback()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use db::backend::memory::MemoryDB;
    use file::{FileFullPath, FileType};

    use super::{IndexInput, Tracker};

    #[test]
    fn test() {
        let db = MemoryDB::default();
        let tracker = Tracker::init(db).unwrap();
        let mut transaction = tracker.start_transaction().unwrap();

        dbg!(transaction
            .index(IndexInput::Directory(
                FileFullPath::parse("/abc/eee"),
                vec![1],
                vec![0],
                vec![
                    ("a".to_string(), FileType::File, vec![2], vec![0]),
                    ("b".to_string(), FileType::File, vec![3], vec![0])
                ],
            ))
            .unwrap());
        dbg!(transaction
            .index(IndexInput::Directory(
                FileFullPath::parse("/abc/eee"),
                vec![1],
                vec![0],
                vec![
                    ("a".to_string(), FileType::File, vec![2], vec![0]),
                    ("b".to_string(), FileType::Directory, vec![4], vec![0])
                ],
            ))
            .unwrap());
        dbg!(transaction
            .index(IndexInput::Directory(
                FileFullPath::parse("/abc/eee/b"),
                vec![4],
                vec![0],
                vec![
                    ("a".to_string(), FileType::Directory, vec![1], vec![0]) // ("b".to_string(), FileType::Directory, vec![4], vec![0])
                ],
            ))
            .unwrap());

        transaction.commit().unwrap();

        println!("{}", tracker.trie());
    }
}
