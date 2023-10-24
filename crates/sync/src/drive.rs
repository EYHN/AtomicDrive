// use std::{path::PathBuf, sync::Arc};

// use chunk::{chunks, HashChunks};
// use crdts::CmRDT;
// use file::{FileEvent, FileType};
// use file_local::{watcher::LocalFileSystemWatcher, LocalFileSystem, LocalFileSystemConfiguration};
// use libp2p::PeerId;
// use parking_lot::Mutex;
// use vindex::{IndexPeerId, VIndex};

// pub struct SyncingDrive {
//     file_system: Arc<LocalFileSystem>,
//     pub vindex: Arc<Mutex<VIndex<HashChunks>>>,
//     watcher: LocalFileSystemWatcher,
// }

// fn apply_file_event(
//     file_event: FileEvent,
//     fs: Arc<LocalFileSystem>,
//     device: &mut VIndex<HashChunks>,
//     device_peer_id: IndexPeerId,
// ) {
//     match file_event.event_type {
//         file::FileEventType::Created | file::FileEventType::Changed => {
//             let stat = fs.stat_file(file_event.path.clone());
//             if stat.file_type == FileType::File {
//                 let chunks = unsafe {
//                     let data = fs.map_file(file_event.path.clone());
//                     chunks(&data)
//                 };
//                 device.apply(device.write(
//                     file_event.path.to_string(),
//                     chunks,
//                     stat.last_write_time,
//                     device_peer_id,
//                     device.read_ctx().derive_add_ctx(device_peer_id),
//                 ))
//             }
//         }
//         file::FileEventType::Deleted => device.apply(device.rm(
//             file_event.path.to_string(),
//             device.read_ctx().derive_add_ctx(device_peer_id),
//         )),
//     }
// }

// impl SyncingDrive {
//     pub fn new(root: PathBuf, data_dir: PathBuf, peer_id: PeerId) -> Self {
//         let fs = Arc::new(LocalFileSystem::new(LocalFileSystemConfiguration {
//             root,
//             data_dir,
//         }));

//         let peer_id = IndexPeerId::from(peer_id);

//         let vindex = Arc::new(Mutex::new(VIndex::default()));

//         let mut vindex_lock = vindex.lock();
//         for event in fs.quick_full_walk() {
//             apply_file_event(event, fs.clone(), &mut vindex_lock, peer_id);
//         }
//         std::mem::drop(vindex_lock);

//         let vindex_for_watch = vindex.clone();
//         let fs_for_watch: Arc<LocalFileSystem> = fs.clone();
//         let watcher = fs.watch(Box::new(move |events| {
//             let mut vindex_lock = vindex_for_watch.lock();
//             for event in events {
//                 apply_file_event(
//                     event,
//                     fs_for_watch.clone(),
//                     &mut vindex_lock,
//                     peer_id,
//                 );
//             }
//         }));

//         Self {
//             file_system: fs,
//             vindex,
//             watcher,
//         }
//     }
// }

// mod debug_ui {
//     use egui::Ui;
//     use utils::tree_stringify;

//     use super::SyncingDrive;

//     impl SyncingDrive {
//         pub fn debug_ui(&self, ui: &mut Ui) {
//             egui::ScrollArea::vertical().max_height(100.0).show(ui, |ui| {
//                 ui.add_sized(
//                     [ui.available_size()[0], 100.0],
//                     egui::TextEdit::multiline(&mut tree_stringify(self.vindex.lock().iter(), "/")),
//                 );
//                 // ui.text_edit_multiline(&mut tree_stringify(self.vindex.lock().iter(), "/"));
//             });
//         }
//     }
// }
