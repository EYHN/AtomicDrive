use std::fmt::Debug;

use crdts::{
    ctx::{AddCtx, ReadCtx, RmCtx},
    CmRDT, Dot, VClock,
};
use libp2p::PeerId;

use crate::VIndexReg;

// #[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
// pub struct IndexedObject {
//     pub content: IndexedFileContent,
//     // pub stats: FileStats,
// }

// impl IndexedObject {
//     fn quick_create_file(content: &str) -> Self {
//         IndexedObject {
//             content: IndexedFileContent::SmallFileContent(
//                 content.as_bytes().to_owned().into_boxed_slice(),
//             ),
//             // stats: Default::default(),
//         }
//     }
// }

// #[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
// pub enum IndexedFileContent {
//     SmallFileContent(Box<[u8]>),
//     ChunkedFile(HashChunks),
// }

#[derive(
    Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct IndexPeerId {
    peer_id: PeerId,
}

impl From<PeerId> for IndexPeerId {
    fn from(value: PeerId) -> Self {
        Self { peer_id: value }
    }
}

type IndexMap<IndexedObject> = crdts::Map<String, VIndexReg<IndexedObject>, IndexPeerId>;
type IndexMapOp<IndexedObject> = <IndexMap<IndexedObject> as CmRDT>::Op;

type VIndexOp<IndexedObject> = (Dot<IndexPeerId>, IndexMapOp<IndexedObject>);

#[derive(Debug, Clone, Default)]
pub struct VIndex<IndexedObject: Clone + Default + Debug + PartialEq> {
    map: IndexMap<IndexedObject>,
    clock: VClock<IndexPeerId>,
    ops: Vec<VIndexOp<IndexedObject>>,
}

impl<IndexedObject: Clone + Default + Debug + PartialEq> VIndex<IndexedObject> {
    fn write(
        &self,
        key: impl Into<String>,
        object: IndexedObject,
        timestamp: u64,
        peer_id: IndexPeerId,
        add_ctx: AddCtx<IndexPeerId>,
    ) -> VIndexOp<IndexedObject> {
        (
            add_ctx.dot.clone(),
            self.map.update(key, add_ctx, |_, add_ctx| {
                VIndexReg::new(object, add_ctx.clock, timestamp, peer_id)
            }),
        )
    }

    fn read(&self, key: impl Into<String>) -> ReadCtx<Option<IndexedObject>, IndexPeerId> {
        let read_ctx = self.map.get(&key.into());

        if let Some(reg) = read_ctx.val {
            ReadCtx {
                add_clock: read_ctx.add_clock,
                rm_clock: read_ctx.rm_clock,
                val: reg.val().cloned(),
            }
        } else {
            ReadCtx {
                add_clock: read_ctx.add_clock,
                rm_clock: read_ctx.rm_clock,
                val: None,
            }
        }
    }

    fn rm(&self, key: impl Into<String>, add_ctx: AddCtx<IndexPeerId>) -> VIndexOp<IndexedObject> {
        (
            add_ctx.dot.clone(),
            self.map.rm(
                key,
                RmCtx {
                    clock: add_ctx.clock,
                },
            ),
        )
    }

    fn read_ctx(&self) -> ReadCtx<(), IndexPeerId> {
        ReadCtx {
            add_clock: self.clock.clone(),
            rm_clock: self.clock.clone(),
            val: (),
        }
    }

    fn clock(&self) -> VClock<IndexPeerId> {
        self.map.read_ctx().add_clock
    }

    fn ops_after(&self, after: &VClock<IndexPeerId>) -> Vec<VIndexOp<IndexedObject>> {
        self.ops
            .iter()
            .filter(|(dot, _)| dot > &after.dot(dot.actor))
            .map(|op| op.clone())
            .collect()
    }

    fn quick_sync(&mut self, other: &mut Self) {
        let other_ops = other.ops_after(&self.clock());

        for op in other_ops {
            self.apply(op);
        }

        let self_ops = self.ops_after(&other.clock());

        for op in self_ops {
            other.apply(op);
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &IndexedObject)> {
        self.map.iter().filter_map(|item_ctx| {
            item_ctx
                .val
                .1
                .val()
                .map(|val| (item_ctx.val.0.as_str(), val))
        })
    }
}

impl<IndexedObject: Clone + Default + Debug + PartialEq> CmRDT for VIndex<IndexedObject> {
    type Op = VIndexOp<IndexedObject>;

    type Validation = <IndexMap<IndexedObject> as CmRDT>::Validation;

    fn validate_op(&self, (_, map_op): &Self::Op) -> Result<(), Self::Validation> {
        self.map.validate_op(map_op)
    }

    fn apply(&mut self, op: Self::Op) {
        self.map.apply(op.1.clone());
        self.clock.apply(op.0);
        self.ops.push(op.clone());
    }
}

#[cfg(test)]
mod tests {
    use crdts::CmRDT;
    use libp2p::PeerId;

    use crate::{IndexPeerId, VIndex};

    #[test]
    fn test_index() {
        let device_1_peer_id = IndexPeerId::from(PeerId::random());
        let device_2_peer_id = IndexPeerId::from(PeerId::random());
        let mut device_1 = VIndex::default();
        device_1.apply(device_1.write(
            "/path/file",
            "test",
            1,
            device_1_peer_id,
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));
        let mut device_2 = device_1.clone();
        device_1.apply(device_1.write(
            "/path/file",
            "123",
            2,
            device_1_peer_id,
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));
        device_2.apply(device_2.write(
            "/path/file",
            "456",
            3,
            device_2_peer_id,
            device_2.read_ctx().derive_add_ctx(device_2_peer_id),
        ));
        device_1.quick_sync(&mut device_2);

        assert_eq!(device_1.read("/path/file").val.unwrap(), "456");
        assert_eq!(device_2.read("/path/file").val.unwrap(), "456");

        device_1.apply(device_1.rm(
            "/path/file",
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));

        device_2.apply(device_2.write(
            "/path/file",
            "789",
            3,
            device_2_peer_id,
            device_2.read_ctx().derive_add_ctx(device_2_peer_id),
        ));

        device_2.quick_sync(&mut device_1);

        assert_eq!(device_1.read("/path/file").val, Some("789"));
        assert_eq!(device_2.read("/path/file").val, Some("789"));

        device_1.apply(device_1.rm(
            "/path/file",
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));

        device_2.quick_sync(&mut device_1);

        assert_eq!(device_1.read("/path/file").val, None);
        assert_eq!(device_2.read("/path/file").val, None);
    }
}
