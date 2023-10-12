//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data associated with the files.

mod discovery;
mod entity;

use db::{DBLock, DBRead, DBTransaction, DBWrite, DB};
use thiserror::Error;
use trie::trie::{Error as TrieError, Op, Trie, TrieId, TrieTransaction};
use utils::{Deserialize, Serialize};

use entity::Entity;

use self::discovery::Discovery;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid Operation, {0}")]
    InvalidOp(String),
    #[error("Operation Ignored, {0}")]
    IgnoredOp(String),
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

/// Store information about whether the file is updated, Usually is a combination of file mtime and size.
type FileUpdateMarker = Vec<u8>;

/// Since we will never conflict, use a simple u128 as the clock
type Clock = u128;

type FileName = String;

pub struct Tracker<DBImpl: DB> {
    db: DBImpl,
}

const DB_TRIE_PREFIX: &[u8] = b"trie:";
const MARKERS_PREFIX: &[u8] = b"mk:";
const CLOCK_KEY: &[u8] = b"current_clock";

impl<DBImpl: DB> Tracker<DBImpl> {
    fn init(db: DBImpl) -> Result<Self> {
        Trie::<Clock, Entity, _>::init(db::DB::prefix(&db, DB_TRIE_PREFIX))?;
        let mut transaction = db.start_transaction()?;
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
    current_ops: Vec<Op<Clock, Entity>>,
}

impl<DBImpl: DBRead + DBWrite + DBLock> TrackerTransaction<DBImpl> {
    fn auto_increment_clock(&mut self) -> Result<Clock> {
        let clock = {
            let bytes = self.db.get_for_update(CLOCK_KEY)?.ok_or(Error::InvalidOp(
                "Tracker Database not initialized.".to_owned(),
            ))?;
            Clock::from_bytes(bytes.as_ref()).map_err(Error::DecodeError)? + 1
        };

        self.db.set(CLOCK_KEY, &clock.to_bytes())?;

        Ok(clock)
    }

    fn trie(&mut self) -> TrieTransaction<Clock, Entity, db::prefix::Prefix<&'_ mut DBImpl>> {
        TrieTransaction::from_db(db::prefix::Prefix::new(&mut self.db, DB_TRIE_PREFIX))
    }

    fn get_marker(&self, file_marker: &FileMarker) -> Result<Option<TrieId>> {
        let mut key = Vec::with_capacity(MARKERS_PREFIX.len() + file_marker.len());
        key.extend_from_slice(MARKERS_PREFIX);
        key.extend_from_slice(file_marker);
        self.db
            .get(key)?
            .map(|d| TrieId::from_bytes(d.as_ref()))
            .transpose()
            .map_err(Error::DecodeError)
    }

    fn set_marker(&mut self, file_marker: &FileMarker, file_id: &TrieId) -> Result<()> {
        let mut key = Vec::with_capacity(MARKERS_PREFIX.len() + file_marker.len());
        key.extend_from_slice(MARKERS_PREFIX);
        key.extend_from_slice(file_marker);
        self.db.set(key, file_id.as_bytes())?;

        Ok(())
    }

    fn lock(&mut self) -> Result<()> {
        self.auto_increment_clock()?;
        self.trie().lock()?;

        Ok(())
    }

    pub fn apply(&mut self, input: Discovery) -> Result<Vec<Op<Clock, Entity>>> {
        let mut target: TrieId;

        if let Some(location_marker) = input.location_marker() {
            if let Some(id) = self.get_marker(location_marker)? {
                target = id;
            } else {
                return Err(Error::InvalidOp("Location not found".to_string()));
            }
        } else if let Some(id) = self
            .trie()
            .get_id_by_path(input.location_full_path().as_ref())?
        {
            target = id;
        } else {
            return Err(Error::InvalidOp("Location not found".to_string()));
        }

        let mut old_entities = vec![];
        for child in self.trie().get_children(target)? {
            old_entities.push(child?);
        }

        for (name, marker, update_marker) in input.entities {
            let old_entity_id = {
                old_entities
                    .iter()
                    .enumerate()
                    .find_map(|(i, entity)| {
                        if entity.0.as_str() == name {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .map(|i| old_entities.remove(i).1)
            };

            if let Some(old_entity_id) = old_entity_id {
                let old_entity = self.trie().get_ensure(old_entity_id)?;
                let old_marker = old_entity.content.marker;
                let old_update_marker = old_entity.content.update_marker;
                if matches!((marker, old_marker), (Some(marker), old_marker) if marker == old_marker)
                {
                    
                }
                if let Some(marker) = marker {
                    if marker == old_entity.content.marker {
                        if update_marker != old_entity.content.update_marker {
                            // update
                        } else {
                            continue;
                        }
                    } else {
                        // move old to recycle, move new here
                    }
                } else if update_marker != old_entity.content.update_marker {
                    // update
                } else {
                    //
                }
            } else {
                // create
            }
        }

        todo!()
    }
}
