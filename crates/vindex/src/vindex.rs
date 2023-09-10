use std::fmt::Debug;

use chunk::HashChunks;
use crdts::{
    ctx::{AddCtx, ReadCtx, RmCtx},
    CmRDT, Dot, VClock,
};
use libp2p::PeerId;
use trie::{backend::memory::TrieMemoryBackend, Trie};
use utils::Digestible;

#[derive(Debug, Hash, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VIndexMarker {
    clock: VClock<PeerId>,
    timestamp: u64,
    peer_id: PeerId,
}

impl PartialOrd for VIndexMarker {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

#[derive(Debug, Hash, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum VIndexContent {
    HashChunks(HashChunks),
    Directory,
}

impl Digestible for VIndexContent {
    fn digest(&self, data: &mut impl utils::Digest) {
        todo!()
    }
}

impl Default for VIndexContent {
    fn default() -> Self {
        Self::Directory
    }
}

type VIndexTrie = Trie<VIndexMarker, VIndexContent, TrieMemoryBackend<VIndexMarker, VIndexContent>>;

#[derive(Clone)]
pub struct VIndex {
    trie: VIndexTrie,
    clock: VClock<PeerId>,
}

impl VIndex {
    pub fn write(
        &mut self,
        path: impl Into<String>,
        object: VIndexContent,
        timestamp: u64,
        add_ctx: AddCtx<PeerId>,
    ) {
        let writer = self.trie.write();
    }

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

    // pub fn read_ctx(&self) -> ReadCtx<(), IndexPeerId> {
    //     ReadCtx {
    //         add_clock: self.clock.clone(),
    //         rm_clock: self.clock.clone(),
    //         val: (),
    //     }
    // }

    // pub fn clock(&self) -> VClock<IndexPeerId> {
    //     self.map.read_ctx().add_clock
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
