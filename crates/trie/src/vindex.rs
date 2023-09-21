use std::{borrow::Borrow, collections::VecDeque, fmt::Debug};

use crate::trie::{Error as TrieError, Op as TrieOp, Trie, TrieUpdater};
use chunk::HashChunks;
use crdts::{CvRDT, VClock};
use db::backend::memory::{MemoryDB, MemoryDBTransaction};
use thiserror::Error;
use utils::{Deserialize, Digestible, Serialize, Serializer};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Trie error")]
    TrieError(#[from] TrieError),
    #[error("db error")]
    DBError(#[from] db::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Hash, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Actor {
    peer_id: libp2p::PeerId,
}

impl Serialize for Actor {
    fn serialize(&self, serializer: Serializer) -> Serializer {
        Vec::<u8>::serialize(&self.peer_id.to_bytes(), serializer)
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.peer_id.to_bytes().len())
    }
}

impl Deserialize for Actor {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (peer_id, rest) = Vec::<u8>::deserialize(bytes)?;

        Ok((
            Actor {
                peer_id: libp2p::PeerId::from_bytes(&peer_id).map_err(|e| e.to_string())?,
            },
            rest,
        ))
    }
}

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Marker {
    clock: VClock<Actor>,
    timestamp: u64,
    actor: Actor,
}

impl Serialize for Marker {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        serializer = self.clock.dots.serialize(serializer);
        serializer = self.timestamp.serialize(serializer);
        serializer = self.actor.serialize(serializer);
        serializer
    }

    fn byte_size(&self) -> Option<usize> {
        Some(self.clock.dots.byte_size()? + self.timestamp.byte_size()? + self.actor.byte_size()?)
    }
}

impl Deserialize for Marker {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        let (dots, bytes) = <_>::deserialize(bytes)?;
        let (timestamp, bytes) = <_>::deserialize(bytes)?;
        let (actor, bytes) = <_>::deserialize(bytes)?;

        Ok((
            Self {
                clock: VClock { dots },
                timestamp,
                actor,
            },
            bytes,
        ))
    }
}

impl PartialOrd for Marker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Entity {
    HashChunks(HashChunks),
    Directory,
}

impl Serialize for Entity {
    fn serialize(&self, mut serializer: Serializer) -> Serializer {
        todo!()
    }

    fn byte_size(&self) -> Option<usize> {
        todo!()
    }
}

impl Deserialize for Entity {
    fn deserialize(bytes: &[u8]) -> std::result::Result<(Self, &[u8]), String> {
        todo!()
    }
}

impl Digestible for Entity {
    fn digest(&self, data: &mut impl utils::Digest) {
        todo!()
    }
}

impl Default for Entity {
    fn default() -> Self {
        Self::Directory
    }
}

type Op = TrieOp<Marker, Entity>;

#[derive(Clone)]
pub struct VIndex {
    db: MemoryDB,
}

impl VIndex {
    fn trie(&self) -> Trie<Marker, Entity, &'_ MemoryDB> {
        Trie::from_db(&self.db)
    }

    pub fn ops_after(&self, after: &VClock<Actor>) -> impl Iterator<Item = Op> {
        let mut result = VecDeque::new();
        for log in self.trie().iter_log().unwrap() {
            let log = log.unwrap();
            let log = log.borrow();
            let log_dot = log.op.marker.clock.dot(log.op.marker.actor.clone());
            if log_dot > after.dot(log_dot.actor.clone()) {
                result.push_front(log.op.clone())
            }
        }

        result.into_iter()
    }

    // fn dir

    // pub fn create_dir_and_(
    //     &mut self,
    //     path: impl Into<String>,
    //     object: Entity,
    //     timestamp: u64,
    //     add_ctx: AddCtx<Actor>,
    // ) {
    //     let writer = self.trie.write();
    // }

    // pub fn read(&self, key: impl Into<String>) -> ReadCtx<Option<IndexedObject>, IndexPeerId> {
    //     let read_ctx = self.map.get(&key.into());

    //     if let Some(reg) = read_ctx.val {
    //         ReadCtx {
    //             add_clock: read_ctx.add_clock,
    //             rm_clock: read_ctx.rm_clock,
    //             val: reg.val().cloned(),
    //         }
    //     } else {
    //         ReadCtx {
    //             add_clock: read_ctx.add_clock,
    //             rm_clock: read_ctx.rm_clock,
    //             val: None,
    //         }
    //     }
    // }

    // pub fn rm(
    //     &self,
    //     key: impl Into<String>,
    //     add_ctx: AddCtx<IndexPeerId>,
    // ) -> VIndexOp<IndexedObject> {
    //     (
    //         add_ctx.dot,
    //         self.map.rm(
    //             key,
    //             RmCtx {
    //                 clock: add_ctx.clock,
    //             },
    //         ),
    //     )
    // }

    // pub fn ops_after(&self, after: &VClock<IndexPeerId>) -> Vec<VIndexOp<IndexedObject>> {
    //     self.ops
    //         .iter()
    //         .filter(|(dot, _)| dot > &after.dot(dot.actor))
    //         .cloned()
    //         .collect()
    // }

    // pub fn quick_sync(&mut self, other: &mut Self) {
    //     let other_ops = other.ops_after(&self.clock());

    //     for op in other_ops {
    //         self.apply(op);
    //     }

    //     let self_ops = self.ops_after(&other.clock());

    //     for op in self_ops {
    //         other.apply(op);
    //     }
    // }

    // pub fn iter(&self) -> impl Iterator<Item = (&str, &IndexedObject)> {
    //     self.map.iter().filter_map(|item_ctx| {
    //         item_ctx
    //             .val
    //             .1
    //             .val()
    //             .map(|val| (item_ctx.val.0.as_str(), val))
    //     })
    // }
}

pub struct VIndexTransaction<'a> {
    db: MemoryDBTransaction<'a>,
}

impl<'a> VIndexTransaction<'a> {
    pub fn clock(&self) -> VClock<Actor> {
        todo!()
    }

    fn update_clock(&self, new_clock: VClock<Actor>) -> Result<()> {
        todo!()
    }

    fn trie(&mut self) -> TrieUpdater<Marker, Entity, &'_ mut MemoryDBTransaction<'a>> {
        TrieUpdater::from_db(&mut self.db)
    }

    fn apply(&mut self, ops: Vec<Op>) -> Result<()> {
        let mut clock = self.clock();
        for op in ops.iter() {
            clock.merge(op.marker.clock.clone())
        }
        self.update_clock(clock)?;
        self.trie().apply(ops)?;

        Ok(())
    }
}

// impl<IndexedObject: Clone + Default + Debug + PartialEq> CmRDT for VIndex<IndexedObject> {
//     type Op = VIndexOp<IndexedObject>;

//     type Validation = <IndexMap<IndexedObject> as CmRDT>::Validation;

//     fn validate_op(&self, (_, map_op): &Self::Op) -> Result<(), Self::Validation> {
//         self.map.validate_op(map_op)
//     }

//     fn apply(&mut self, op: Self::Op) {
//         self.map.apply(op.1.clone());
//         self.clock.apply(op.0);
//         self.ops.push(op);
//     }
// }

// #[cfg(test)]
// mod tests {
//     use crdts::CmRDT;
//     use libp2p::PeerId;

//     use crate::{IndexPeerId, VIndex};

//     #[test]
//     fn test_index() {
//         let device_1_peer_id = IndexPeerId::from(PeerId::random());
//         let device_2_peer_id = IndexPeerId::from(PeerId::random());
//         let mut device_1 = VIndex::default();
//         device_1.apply(device_1.write(
//             "/path/file",
//             "test",
//             1,
//             device_1_peer_id,
//             device_1.read_ctx().derive_add_ctx(device_1_peer_id),
//         ));
//         let mut device_2 = device_1.clone();
//         device_1.apply(device_1.write(
//             "/path/file",
//             "123",
//             2,
//             device_1_peer_id,
//             device_1.read_ctx().derive_add_ctx(device_1_peer_id),
//         ));
//         device_2.apply(device_2.write(
//             "/path/file",
//             "456",
//             3,
//             device_2_peer_id,
//             device_2.read_ctx().derive_add_ctx(device_2_peer_id),
//         ));
//         device_1.quick_sync(&mut device_2);

//         assert_eq!(device_1.read("/path/file").val.unwrap(), "456");
//         assert_eq!(device_2.read("/path/file").val.unwrap(), "456");

//         device_1.apply(device_1.rm(
//             "/path/file",
//             device_1.read_ctx().derive_add_ctx(device_1_peer_id),
//         ));

//         device_2.apply(device_2.write(
//             "/path/file",
//             "789",
//             3,
//             device_2_peer_id,
//             device_2.read_ctx().derive_add_ctx(device_2_peer_id),
//         ));

//         device_2.quick_sync(&mut device_1);

//         assert_eq!(device_1.read("/path/file").val, Some("789"));
//         assert_eq!(device_2.read("/path/file").val, Some("789"));

//         device_1.apply(device_1.rm(
//             "/path/file",
//             device_1.read_ctx().derive_add_ctx(device_1_peer_id),
//         ));

//         device_2.quick_sync(&mut device_1);

//         assert_eq!(device_1.read("/path/file").val, None);
//         assert_eq!(device_2.read("/path/file").val, None);
//     }
// }
