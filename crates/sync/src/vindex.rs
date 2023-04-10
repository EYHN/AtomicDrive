use crdts::{
    ctx::{AddCtx, ReadCtx},
    CmRDT, LWWReg, VClock,
};
use file::{FileFullPath, FileStats};
use libp2p::PeerId;

use crate::FileHashChunks;

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



type IndexMap = crdts::Map<FileFullPath, LWWReg<IndexedFile, IndexOpMarker>, IndexPeerId>;
type IndexMapOp = <IndexMap as CmRDT>::Op;

#[derive(Debug, Clone, Default)]
pub struct VIndex {
    map: IndexMap,
    ops: Vec<IndexMapOp>,
}

impl VIndex {
    fn write(
        &self,
        full_path: impl Into<FileFullPath>,
        file: IndexedFile,
        marker: IndexOpMarker,
        add_ctx: AddCtx<IndexPeerId>,
        item_add_ctx: Option<AddCtx<IndexPeerId>>,
    ) -> IndexMapOp {
        self.map.update(full_path, add_ctx, |v, add_ctx| {
            v.write(file, marker, item_add_ctx.unwrap_or(add_ctx))
        })
    }

    fn read(
        &self,
        full_path: &FileFullPath,
    ) -> ReadCtx<Option<ReadCtx<Option<IndexedFile>, IndexPeerId>>, IndexPeerId> {
        let read_ctx = self.map.get(&full_path);

        if let Some(reg) = read_ctx.val {
            ReadCtx {
                add_clock: read_ctx.add_clock,
                rm_clock: read_ctx.rm_clock,
                val: Some(reg.read_single()),
            }
        } else {
            ReadCtx {
                add_clock: read_ctx.add_clock,
                rm_clock: read_ctx.rm_clock,
                val: None,
            }
        }
    }

    fn read_mutli(&self, full_path: &FileFullPath) -> ReadCtx<Vec<IndexedFile>, IndexPeerId> {
        let read_ctx = self.map.get(&full_path);

        if let Some(reg) = read_ctx.val {
            return reg.read();
        }

        ReadCtx {
            add_clock: read_ctx.add_clock,
            rm_clock: read_ctx.rm_clock,
            val: Vec::new(),
        }
    }

    fn read_ctx(&self) -> ReadCtx<(), IndexPeerId> {
        self.map.read_ctx()
    }

    fn clock(&self) -> VClock<IndexPeerId> {
        self.map.read_ctx().add_clock
    }

    fn derive_create_ctx(
        &self,
        read_ctx: ReadCtx<(), IndexPeerId>,
        peer_id: IndexPeerId,
    ) -> AddCtx<IndexPeerId> {
        let base_clock = read_ctx.add_clock.clone();
        let mut add_ctx = read_ctx.derive_add_ctx(peer_id);
        add_ctx.clock = add_ctx.clock.clone_without(&base_clock);
        add_ctx
    }
}

impl CmRDT for VIndex {
    type Op = IndexMapOp;

    type Validation = <IndexMap as CmRDT>::Validation;

    fn validate_op(&self, op: &Self::Op) -> Result<(), Self::Validation> {
        self.map.validate_op(op)
    }

    fn apply(&mut self, op: Self::Op) {
        self.map.apply(op.clone());
        self.ops.push(op);
    }
}

#[cfg(test)]
mod tests {
    use crdts::CmRDT;
    use file::FileFullPath;
    use libp2p::PeerId;

    use crate::{IndexOpMarker, IndexPeerId, IndexedFile, VIndex};

    #[test]
    fn test_index() {
        let device_1_peer_id = IndexPeerId::from(PeerId::random());
        let device_2_peer_id = IndexPeerId::from(PeerId::random());
        let file_path = FileFullPath::parse("/path/file");
        let mut device_1 = VIndex::default();
        device_1.apply(device_1.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "test"),
            IndexOpMarker {
                timestamp: 1,
                peer_id: device_1_peer_id,
            },
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
            None,
        ));
        let mut device_2 = device_1.clone();
        let device_1_write = device_1.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "123"),
            IndexOpMarker {
                timestamp: 2,
                peer_id: device_1_peer_id,
            },
            device_1.read_ctx().derive_add_ctx(device_1_peer_id),
            None,
        );
        device_1.apply(device_1_write.clone());
        let device_2_write = device_2.write(
            file_path.clone(),
            IndexedFile::quick_create_file(file_path.clone(), "456"),
            IndexOpMarker {
                timestamp: 3,
                peer_id: device_2_peer_id,
            },
            device_2.read_ctx().derive_add_ctx(device_2_peer_id),
            None,
        );
        device_2.apply(device_2_write.clone());
        device_1.apply(device_2_write);
        device_2.apply(device_1_write);

        assert_eq!(
            device_1.read(&file_path).val.unwrap().val.unwrap(),
            IndexedFile::quick_create_file(file_path.clone(), "456")
        );
        assert_eq!(
            device_2.read(&file_path).val.unwrap().val.unwrap(),
            IndexedFile::quick_create_file(file_path.clone(), "456")
        );
        assert_eq!(
            device_1.read_mutli(&file_path).val,
            vec![
                IndexedFile::quick_create_file(file_path.clone(), "123"),
                IndexedFile::quick_create_file(file_path.clone(), "456")
            ]
        );
        assert_eq!(
            device_2.read_mutli(&file_path).val,
            vec![
                IndexedFile::quick_create_file(file_path.clone(), "456"),
                IndexedFile::quick_create_file(file_path.clone(), "123"),
            ]
        );

        dbg!(device_1.read(&file_path));
        dbg!(device_1.map.get(&file_path));
        device_1.apply(
            device_1.write(
                file_path.clone(),
                IndexedFile::quick_create_file(file_path.clone(), "789"),
                IndexOpMarker {
                    timestamp: 4,
                    peer_id: device_1_peer_id,
                },
                device_1.read_ctx().derive_add_ctx(device_1_peer_id),
                Some(
                    device_1
                        .read(&file_path)
                        .val
                        .unwrap()
                        .derive_add_ctx(device_1_peer_id),
                ),
            ),
        );
        assert_eq!(
            device_1.read_mutli(&file_path).val,
            vec![
                IndexedFile::quick_create_file(file_path.clone(), "123"),
                IndexedFile::quick_create_file(file_path.clone(), "789")
            ]
        );
    }
}
