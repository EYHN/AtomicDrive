// mod drive;

// pub use drive::SyncingDrive;

pub struct SyncingDrive {
    file_system: Arc<LocalFileSystem>,
    pub vindex: Arc<Mutex<VIndex<HashChunks>>>,
    watcher: LocalFileSystemWatcher,
}

// use std::path::PathBuf;

// use chunk::chunks;
// use crdts::CmRDT;
// use file::FileType;
// use file_local::{LocalFileSystem, LocalFileSystemConfiguration};
// use libp2p::PeerId;
// use utils::tree_stringify;
// use vindex::{IndexPeerId, VIndex};

// fn main() {
//     let fs = LocalFileSystem::new(LocalFileSystemConfiguration {
//         root: PathBuf::from("/Users/admin/Projects/AtomicDrive"),
//         data_dir: PathBuf::from("/Users/admin/Projects/AtomicDrive/cache"),
//     });

//     let device_1_peer_id = IndexPeerId::from(PeerId::random());

//     let mut device_1 = VIndex::default();

//     for event in fs.quick_full_walk() {
//         match event.event_type {
//             file::FileEventType::Created => {
//                 let stat = fs.stat_file(event.path.clone());
//                 if stat.file_type == FileType::File {
//                     let data = fs.read_file(event.path.clone());
//                     device_1.apply(device_1.write(
//                         event.path.to_string(),
//                         chunks(&data),
//                         stat.last_write_time,
//                         device_1_peer_id,
//                         device_1.read_ctx().derive_add_ctx(device_1_peer_id),
//                     ))
//                 }
//             }
//             file::FileEventType::Deleted => todo!(),
//             file::FileEventType::Changed => todo!(),
//         }
//     }

//     println!("{}", tree_stringify(device_1.iter(), "/"));
// }
