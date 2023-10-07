//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

use std::{borrow::Borrow, f32::consts::E, path::PathBuf};

use db::{DBLock, DBRead, DBWrite, DB};
use file::{FileFullPath, FileType};
use thiserror::Error;
use trie::trie::{
    Error as TrieError, Op, OpTarget, Trie, TrieId, TrieKey, TrieRef, TrieTransaction, RECYCLE,
    RECYCLE_REF, ROOT,
};
use utils::{Deserialize, Digest, Digestible, PathTools, Serialize};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
    #[error("Decode error, {0}")]
    DecodeError(String),
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
type FileId = Vec<u8>;

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

pub struct Tracker<DBImpl: DB> {
    db: DBImpl,
}

impl<DBImpl: DB> Tracker<DBImpl> {
    fn trie(&self) -> Trie<u128, Entity, db::prefix::Prefix<&'_ DBImpl>> {
        Trie::from_db(db::DB::prefix(&self.db, DB_TRIE_PREFIX))
    }

    // // fn get_file_status_on_vindex(
    // //     &self,
    // //     trie_id: TrieId,
    // // ) -> Result<(FileIdentifier, FileUpdateToken)> {
    // //     todo!()
    // // }

    // // fn get_id_by_full_path(&self, full_path: FileFullPath) -> Result<Option<TrieId>> {
    // //     todo!()
    // // }

    // pub fn index(&self, input: IndexInput) -> Result<()> {
    //     let mut trie = self.trie();
    //     let trie_write = trie.write().unwrap();
    //     if let IndexInput::Empty(full_path) = input {
    //         trie_write.get_id_by_path(full_path)
    //     }

    //     let full_path = match input {
    //         IndexInput::File(file_path, _, _, _) => file_path,
    //         IndexInput::Directory(file_path, _, _, _) => file_path,
    //         IndexInput::Empty(file_path) => file_path,
    //     };

    //     let parent_full_path = full_path.dirname();
    //     if let Some(parent_id) = self.get_id_by_full_path(parent_full_path)? {
    //         let
    //     } else {
    //     todo!()
    // }

    //     dbg!(full_path);

    //     todo!()
    // }
}

pub struct TrackerTransaction<DBImpl: DBRead + DBWrite + DBLock> {
    db: DBImpl,
    current_ops: Vec<Op<u128, Entity>>,
}

impl<DBImpl: DBRead + DBWrite + DBLock> TrackerTransaction<DBImpl> {
    fn lock(&mut self) -> Result<()> {
        self.auto_increment_clock()?;
        self.trie().lock();

        Ok(())
    }

    fn trie(&mut self) -> TrieTransaction<u128, Entity, db::prefix::Prefix<&'_ mut DBImpl>> {
        TrieTransaction::from_db(db::prefix::Prefix::new(&mut self.db, DB_TRIE_PREFIX))
    }

    fn trie_id_marker_index_get() -> TrieId {

    }

    fn trie_id_marker_index_set() -> Result<()> {

    }

    fn auto_increment_file_id(&mut self) -> Result<FileId> {
        let old_file_id = {
            let bytes = self
                .db
                .get(AUTO_INCREMENT_FILE_ID_KEY)?
                .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?;

            u128::from_be_bytes(
                bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::DecodeError("failed to decode file id".to_string()))?,
            )
        };

        let new_file_id = (old_file_id + 1).to_be_bytes().to_vec();

        self.db.set(AUTO_INCREMENT_FILE_ID_KEY, &new_file_id)?;

        Ok(new_file_id)
    }

    fn auto_increment_clock(&mut self) -> Result<Clock> {
        let clock = {
            let bytes = self
                .db
                .get_for_update(CLOCK_KEY)?
                .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?;
            Clock::from_bytes(bytes.as_ref()).map_err(Error::DecodeError)? + 1
        };

        self.db.set(CLOCK_KEY, &clock.to_bytes())?;

        Ok(clock)
    }

    fn move_node(&mut self, node: TrieId, to: TrieId, key: TrieKey) -> Result<()> {
        let new_id = self.auto_increment_clock()?;

        self.apply(Op {
            marker: new_id,
            parent_target: to.into(),
            child_key: key,
            child_target: node.into(),
            child_content: None,
        })
    }

    fn move_node_to_recycle(&mut self, node: TrieId) -> Result<()> {
        self.move_node(node, RECYCLE, TrieKey(node.id().to_string()))
    }

    fn create_node(&mut self, to: TrieId, key: TrieKey, content: Entity) -> Result<()> {
        let new_id = self.auto_increment_clock()?;
        self.apply(Op {
            marker: new_id,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::NewId,
            child_content: Some(content),
        })
    }

    fn update_node(
        &mut self,
        to: TrieId,
        key: TrieKey,
        id: TrieId,
        old_content: &Entity,
        marker: FileMarker,
        update_marker: FileUpdateMarker,
    ) -> Result<()> {
        let new_id = self.auto_increment_clock()?;
        self.apply(Op {
            marker: new_id,
            parent_target: to.into(),
            child_key: key,
            child_target: OpTarget::Id(id),
            child_content: Some(Entity {
                file_id: old_content.file_id.clone(),
                is_directory: old_content.is_directory,
                marker,
                update_marker,
            }),
        })
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
                        let new_dir = Entity::empty_directory(self.auto_increment_file_id()?);
                        self.create_node(parent_id, part.to_owned().into(), new_dir)?;
                        parent_id = self
                            .trie()
                            .get_child_ensure(parent_id, TrieKey(part.to_owned()))?;
                    } else {
                        parent_id = node_id
                    }
                } else {
                    // if not exists, create a directory
                    let new_dir = Entity::empty_directory(self.auto_increment_file_id()?);
                    self.create_node(parent_id, part.to_owned().into(), new_dir)?;
                    parent_id = self
                        .trie()
                        .get_child_ensure(parent_id, TrieKey(part.to_owned()))?;
                }
            }

            if let IndexInput::File(full_path, _, file_marker, file_update_marker) = input {
                let file_name = PathTools::basename(full_path.as_ref());
                let old_child_id = self
                    .trie()
                    .get_child(parent_id, file_name.to_owned().into())?;

                if let Some(old_child_id) = old_child_id {
                    let old_child = self.trie().get_ensure(old_child_id)?;
                    if old_child.content.is_directory {
                        self.move_node_to_recycle(old_child_id)?;
                        let new_file = Entity::file(
                            self.auto_increment_file_id()?,
                            file_marker,
                            file_update_marker,
                        );
                        self.create_node(parent_id, file_name.to_owned().into(), new_file)?;
                    } else {
                        // old is file
                        if old_child.content.marker != file_marker
                            || old_child.content.update_marker != file_update_marker
                        {
                            self.update_node(
                                parent_id,
                                file_name.to_owned().into(),
                                old_child_id,
                                &old_child.content,
                                file_marker,
                                file_update_marker,
                            )?;
                        }
                    }
                }
            } else if let IndexInput::Directory(
                full_path,
                dir_marker,
                dir_update_marker,
                children,
            ) = input
            {
                
            } else {
                unreachable!()
            }

            todo!()
        }
    }
}
