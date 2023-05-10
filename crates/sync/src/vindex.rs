use crdts::{
    ctx::{AddCtx, ReadCtx, RmCtx},
    CmRDT, Dot, VClock,
};
use file::{FileFullPath, FileStats};
use libp2p::PeerId;

use crate::{FileHashChunks, VIndexReg};

#[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IndexedFile {
    pub path: FileFullPath,
    pub content: IndexedFileContent,
    pub stats: FileStats,
}

impl IndexedFile {
    fn quick_create_file(path: FileFullPath, content: &str) -> Self {
        IndexedFile {
            path,
            content: IndexedFileContent::SmallFileContent(
                content.as_bytes().to_owned().into_boxed_slice(),
            ),
            stats: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum IndexedFileContent {
    SmallFileContent(Box<[u8]>),
    ChunkedFile(FileHashChunks),
}

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

type IndexMap = crdts::Map<FileFullPath, VIndexReg, IndexPeerId>;
type IndexMapOp = <IndexMap as CmRDT>::Op;

type VIndexOp = (Dot<IndexPeerId>, IndexMapOp);

#[derive(Debug, Clone, Default)]
pub struct VIndex {
    map: IndexMap,
    clock: VClock<IndexPeerId>,
    ops: Vec<VIndexOp>,
}

impl VIndex {
    fn write(
        &self,
        full_path: impl Into<FileFullPath>,
        file: IndexedFile,
        timestamp: u64,
        peer_id: IndexPeerId,
        add_ctx: AddCtx<IndexPeerId>,
    ) -> VIndexOp {
        (
            add_ctx.dot.clone(),
            self.map.update(full_path, add_ctx, |_, add_ctx| {
                VIndexReg::new(file, add_ctx.clock, timestamp, peer_id)
            }),
        )
    }

    fn read(&self, full_path: &FileFullPath) -> ReadCtx<Option<IndexedFile>, IndexPeerId> {
        let read_ctx = self.map.get(full_path);

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

    fn rm(&self, full_path: impl Into<FileFullPath>, add_ctx: AddCtx<IndexPeerId>) -> VIndexOp {
        (
            add_ctx.dot.clone(),
            self.map.rm(
                full_path,
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

    fn ops_after(&self, after: &VClock<IndexPeerId>) -> Vec<VIndexOp> {
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
}

impl CmRDT for VIndex {
    type Op = VIndexOp;

    type Validation = <IndexMap as CmRDT>::Validation;

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
    use file::FileFullPath;
    use libp2p::PeerId;

    use crate::{IndexPeerId, IndexedFile, VIndex};

    #[test]
    fn test_index() {
        let device_1_peer_id = IndexPeerId::from(PeerId::random());
        let device_2_peer_id = IndexPeerId::from(PeerId::random());
        let file_path = FileFullPath::parse("/path/file");
        let mut device_1 = VIndex::default();
        device_1.apply(device_1.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "test"),
            1,
            device_1_peer_id,
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));
        let mut device_2 = device_1.clone();
        device_1.apply(device_1.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "123"),
            2,
            device_1_peer_id,
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));
        device_2.apply(device_2.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "456"),
            3,
            device_2_peer_id,
            device_2.read_ctx().derive_add_ctx(device_2_peer_id),
        ));
        device_1.quick_sync(&mut device_2);

        assert_eq!(
            device_1.read(&file_path).val.unwrap(),
            IndexedFile::quick_create_file(file_path.clone(), "456")
        );
        assert_eq!(
            device_2.read(&file_path).val.unwrap(),
            IndexedFile::quick_create_file(file_path.clone(), "456")
        );

        device_1.apply(device_1.rm(
            file_path.clone(),
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));

        device_2.apply(device_2.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "789"),
            3,
            device_2_peer_id,
            device_2.read_ctx().derive_add_ctx(device_2_peer_id),
        ));

        device_2.quick_sync(&mut device_1);

        assert_eq!(
            device_1.read(&file_path).val,
            Some(IndexedFile::quick_create_file(file_path.clone(), "789"))
        );
        assert_eq!(
            device_2.read(&file_path).val,
            Some(IndexedFile::quick_create_file(file_path.clone(), "789"))
        );

        device_1.apply(device_1.rm(
            file_path.clone(),
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
        ));

        device_2.quick_sync(&mut device_1);

        assert_eq!(device_1.read(&file_path).val, None);
        assert_eq!(device_2.read(&file_path).val, None);
    }
}
