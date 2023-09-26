//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

use std::{borrow::Borrow, f32::consts::E, path::PathBuf};

use db::{DBLock, DBRead, DBWrite, DB};
use file::{FileFullPath, FileType};
use thiserror::Error;
use trie::trie::{
    Error as TrieError, Op, Trie, TrieId, TrieKey, TrieTransaction, RECYCLE_REF, ROOT, TrieRef,
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

/// store file system level file identifier, e.g. inode in linux, file_id in windows
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
struct Entity {
    file_id: FileId,
    marker: FileMarker,
    update_marker: FileUpdateMarker,
    is_directory: bool,
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
}

impl<DBImpl: DBRead + DBWrite + DBLock> TrackerTransaction<DBImpl> {
    fn trie(
        &mut self,
    ) -> Result<TrieTransaction<u128, Entity, db::prefix::Prefix<&'_ mut DBImpl>>> {
        Ok(TrieTransaction::from_db(db::prefix::Prefix::new(
            &mut self.db,
            DB_TRIE_PREFIX,
        ))?)
    }

    pub fn auto_increment_file_id(&mut self) -> Result<FileId> {
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

    pub fn clock(&self) -> Result<Clock> {
        let bytes = self
            .db
            .get_for_update(CLOCK_KEY)?
            .ok_or(Error::InvalidOp("Database not initialized.".to_owned()))?;

        Clock::from_bytes(bytes.as_ref()).map_err(Error::DecodeError)
    }

    pub fn update_clock(&mut self, new_clock: Clock) -> Result<()> {
        Ok(self.db.set(CLOCK_KEY, new_clock.to_bytes())?)
    }

    pub fn index(&mut self, input: IndexInput) -> Result<Vec<Op<u128, Entity>>> {
        let mut clock = self.clock()?;
        let mut trie = self.trie()?;

        if let IndexInput::Empty(full_path) = input {
            if let Some(old_file_id) = trie.get_id_by_path(full_path.as_ref())? {
                clock += 1;
                let ops = vec![Op {
                    marker: clock,
                    parent_ref: RECYCLE_REF,
                    child_key: TrieKey(old_file_id.id().to_string()),
                    child_content: trie.get_ensure(old_file_id)?.content,
                    child_ref: trie.get_first_ref_ensure(old_file_id)?,
                }];
                trie.apply(ops.clone())?;
                self.update_clock(clock)?;
                Ok(ops)
            } else {
                Ok(vec![])
            }
        } else {
            let mut ops = vec![];
            let full_path = match &input {
                IndexInput::File(full_path, _, _, _) => full_path,
                IndexInput::Directory(full_path, _, _, _) => full_path,
                IndexInput::Empty(_) => unreachable!(),
            };

            let mut parent_id = ROOT;
            for part in PathTools::parts(full_path.dirname().as_ref()) {
                if let Some(child_id) = trie.get_child(parent_id, TrieKey(part.to_owned()))? {
                    let child = trie.get_ensure(child_id)?;
                    if !child.content.is_directory {
                        clock += 1;
                        let delete_op = Op {
                            marker: clock,
                            parent_ref: RECYCLE_REF,
                            child_key: TrieKey(child_id.id().to_string()),
                            child_content: trie.get_ensure(child_id)?.content,
                            child_ref: trie.get_first_ref_ensure(child_id)?,
                        };
                        clock += 1;
                        let new_child_ref = TrieRef::new();
                        let create_op = Op {
                            marker: clock,
                            parent_ref: trie.get_first_ref_ensure(parent_id)?,
                            child_key: TrieKey(part.to_owned()),
                            child_content: Entity {
                                file_id: self.auto_increment_file_id()?,
                                marker: Default::default(),
                                update_marker: Default::default(),
                                is_directory: true
                            },
                            child_ref: new_child_ref
                        };
                        trie.apply(vec![delete_op.clone(), create_op.clone()])?;
                        ops.push(vec![delete_op, create_op]);
                        parent_id = trie.get_id_ensure(new_child_ref)?;
                    } else {
                    parent_id = child_id
                }
                } else {
                    let create_op = Op {
                        marker: clock,
                        parent_ref: trie.get_first_ref_ensure(parent_id)?,
                        child_key: TrieKey(part.to_owned()),
                        child_content: Entity {
                            file_id: self.auto_increment_file_id()?,
                            marker: Default::default(),
                            update_marker: Default::default(),
                            is_directory: true
                        },
                        child_ref: new_child_ref
                    };
                    trie.apply(vec![create_op.clone()])?;
                    ops.push(vec![create_op]);
                    parent_id = trie.get_id_ensure(new_child_ref)?;
                }
            }

            todo!()
        }
    }
}
