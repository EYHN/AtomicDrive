use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use chunk::{chunks, HashChunks};
use crdts::CmRDT;
use file::{FileEvent, FileType};
use file_local::{watcher::LocalFileSystemWatcher, LocalFileSystem, LocalFileSystemConfiguration};
use libp2p::PeerId;
use utils::{tree_stringify, workspace};
use vindex::{IndexPeerId, VIndex};

pub struct SyncingApp {
    file_system: Arc<LocalFileSystem>,
    device: Arc<Mutex<VIndex<HashChunks>>>,
    watcher: LocalFileSystemWatcher,
}

fn apply_file_event(
    file_event: FileEvent,
    fs: Arc<LocalFileSystem>,
    device: &mut VIndex<HashChunks>,
    device_peer_id: IndexPeerId,
) {
    match file_event.event_type {
        file::FileEventType::Created | file::FileEventType::Changed => {
            let stat = fs.stat_file(file_event.path.clone());
            if stat.file_type == FileType::File {
                let data = unsafe { fs.map_file(file_event.path.clone()) };
                device.apply(device.write(
                    file_event.path.to_string(),
                    chunks(&data),
                    stat.last_write_time,
                    device_peer_id,
                    device.read_ctx().derive_add_ctx(device_peer_id),
                ))
            }
        }
        file::FileEventType::Deleted => device.apply(device.rm(
            file_event.path.to_string(),
            device.read_ctx().derive_add_ctx(device_peer_id),
        )),
    }
}

impl SyncingApp {
    pub fn new() -> Self {
        let fs = Arc::new(LocalFileSystem::new(LocalFileSystemConfiguration {
            root: PathBuf::from(workspace!("test_dir")),
            data_dir: PathBuf::from(workspace!("cache")),
        }));

        let device_peer_id = IndexPeerId::from(PeerId::random());

        let device = Arc::new(Mutex::new(VIndex::default()));

        let mut device_lock: std::sync::MutexGuard<VIndex<HashChunks>> = device.lock().unwrap();
        for event in fs.quick_full_walk() {
            apply_file_event(event, fs.clone(), &mut device_lock, device_peer_id.clone());
        }
        std::mem::drop(device_lock);

        let device_for_watch = device.clone();
        let fs_for_watch: Arc<LocalFileSystem> = fs.clone();
        let watcher = fs.watch(Box::new(move |events| {
            let mut device_lock: std::sync::MutexGuard<VIndex<HashChunks>> =
                device_for_watch.lock().unwrap();
            for event in events {
                apply_file_event(
                    event,
                    fs_for_watch.clone(),
                    &mut device_lock,
                    device_peer_id.clone(),
                );
            }
        }));

        Self {
            file_system: fs,
            device,
            watcher,
        }
    }
}

impl eframe::App for SyncingApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(&ctx, |ui| {
            ui.add(egui::Label::new("Hello World!"));
            ui.label("A shorter and more convenient way to add a label.");
            ui.text_edit_multiline(&mut tree_stringify(self.device.lock().unwrap().iter(), "/"));
            if ui.button("Click me").clicked() {
                // take some action here
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    tracing_subscriber::fmt::init();

    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Atomic Drive",
        native_options,
        Box::new(|cc| {
            let mut fonts = egui::FontDefinitions::default();

            fonts.font_data.insert(
                "FiraCode".to_owned(),
                egui::FontData::from_static(include_bytes!("FiraCode-Regular.ttf")),
            );

            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .insert(0, "FiraCode".to_owned());

            cc.egui_ctx.set_fonts(fonts);
            Box::new(SyncingApp::new())
        }),
    )
}
