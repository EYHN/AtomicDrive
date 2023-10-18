//! database for local file system provider.
//!
//! The local file system not have a way to save metadata, tags, notes on files.
//! We use an additional database to track local files and store the data
//! associated with the files.

mod discovery;
mod entity;
mod marker;

pub use discovery::*;
pub use entity::*;
pub use marker::*;

use db::{DBLock, DBRead, DBTransaction, DBWrite, DB};
use thiserror::Error;
use trie::trie::{Error as TrieError, Op, OpTarget, Trie, TrieId, TrieTransaction};
use utils::{Deserialize, Serialize};

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
    pub fn init(db: DBImpl) -> Result<Self> {
        Trie::<Clock, Entity, _>::init(db::DB::prefix(&db, DB_TRIE_PREFIX))?;
        let mut transaction = db.start_transaction()?;
        if !transaction.has(CLOCK_KEY)? {
            transaction.set(CLOCK_KEY, 0u128.to_bytes())?;
        }
        transaction.commit()?;
        Ok(Tracker { db })
    }

    pub fn trie(&self) -> Trie<u128, Entity, db::prefix::Prefix<&'_ DBImpl>> {
        Trie::from_db(db::DB::prefix(&self.db, DB_TRIE_PREFIX))
    }

    pub fn start_transaction(&self) -> Result<TrackerTransaction<DBImpl::Transaction<'_>>> {
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
    fn do_op(&mut self, op: Op<Clock, Entity>) -> Result<()> {
        self.trie().apply(vec![op.clone()])?;
        self.current_ops.push(op);
        Ok(())
    }

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

    fn delete_marker(&mut self, file_marker: &FileMarker) -> Result<()> {
        let mut key = Vec::with_capacity(MARKERS_PREFIX.len() + file_marker.len());
        key.extend_from_slice(MARKERS_PREFIX);
        key.extend_from_slice(file_marker);
        self.db.delete(key)?;

        Ok(())
    }

    fn move_node_to_recycle(&mut self, node: TrieId) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.do_op(Op {
            marker: new_clock,
            parent_target: trie::trie::RECYCLE.into(),
            child_key: node.id().to_string().into(),
            child_target: node.into(),
            child_content: None,
        })
    }

    fn move_exist_entity_to(
        &mut self,
        parent: TrieId,
        entity: DiscoveryEntity,
        exist_id: TrieId,
    ) -> Result<()> {
        let new_clock = self.auto_increment_clock()?;

        self.do_op(Op {
            marker: new_clock,
            parent_target: OpTarget::Id(parent),
            child_key: entity.name.into(),
            child_target: OpTarget::Id(exist_id),
            child_content: Some(Entity {
                marker: entity.marker,
                update_marker: entity.update_marker,
                type_marker: entity.type_marker,
            }),
        })?;

        Ok(())
    }

    fn move_entity_to(&mut self, parent: TrieId, entity: DiscoveryEntity) -> Result<TrieId> {
        let new_clock = self.auto_increment_clock()?;

        let target_id = self.trie().create_id()?;

        self.do_op(Op {
            marker: new_clock,
            parent_target: OpTarget::Id(parent),
            child_key: entity.name.into(),
            child_target: OpTarget::Id(target_id),
            child_content: Some(Entity {
                marker: entity.marker,
                update_marker: entity.update_marker,
                type_marker: entity.type_marker,
            }),
        })?;

        Ok(target_id)
    }

    fn lock(&mut self) -> Result<()> {
        self.auto_increment_clock()?;
        self.trie().lock()?;

        Ok(())
    }

    pub fn apply(&mut self, input: Discovery) -> Result<Vec<Op<Clock, Entity>>> {
        self.lock()?;

        let target: TrieId;

        if !input.location_marker().is_empty() {
            if let Some(id) = self.get_marker(input.location_marker())? {
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

        let mut entities = vec![];
        for entity in input.entities {
            if !entity.marker.is_empty() {
                let marker = self.get_marker(&entity.marker)?;
                entities.push((entity, marker))
            } else {
                entities.push((entity, None))
            }
        }

        for (entity, exist_id) in entities {
            let old_entity_id = {
                old_entities
                    .iter()
                    .enumerate()
                    .find_map(|(i, (key, _))| {
                        if key.as_str() == entity.name {
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
                let marker = if entity.marker.is_empty() {
                    old_marker.clone()
                } else {
                    entity.marker.clone()
                }; // if marker is empty, same as old_marker
                let old_update_marker = &old_entity.content.update_marker;
                let update_marker = &entity.update_marker;
                let old_type_marker = &old_entity.content.type_marker;
                let type_marker = &entity.type_marker;

                if marker == old_marker && type_marker == old_type_marker {
                    if update_marker != old_update_marker {
                        // update
                        self.move_exist_entity_to(target, entity, old_entity_id)?;
                    }

                    continue;
                } else {
                    // move old to recycle, move new here
                    self.move_node_to_recycle(old_entity_id)?;
                }
            }

            if let Some(exist_id) = exist_id {
                self.move_exist_entity_to(target, entity, exist_id)?;

                if let Some(i) = old_entities.iter().enumerate().find_map(|(i, (_, id))| {
                    if id == &exist_id {
                        Some(i)
                    } else {
                        None
                    }
                }) {
                    old_entities.remove(i);
                }
            } else {
                let marker = entity.marker.clone();
                let new_id = self.move_entity_to(target, entity)?;
                if !marker.is_empty() {
                    self.set_marker(&marker, &new_id)?;
                }
            }
        }

        for (_, old_entity_id) in old_entities {
            self.move_node_to_recycle(old_entity_id)?;
        }

        Ok(core::mem::take(&mut self.current_ops))
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
