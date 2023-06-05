use std::path::PathBuf;

use chunk::{chunks, HashChunks};
use crdts::CmRDT;
use file::FileType;
use file_local::{LocalFileSystem, LocalFileSystemConfiguration};
use libp2p::PeerId;
use utils::tree_stringify;
use vindex::{IndexPeerId, VIndex};

pub struct SyncingApp {
    file_system: LocalFileSystem,
    device: VIndex<HashChunks>,
}

impl SyncingApp {
    pub fn new() -> Self {
        let fs = LocalFileSystem::new(LocalFileSystemConfiguration {
            root: PathBuf::from("/Users/admin/Projects/AtomicDrive/test_dir"),
            data_dir: PathBuf::from("/Users/admin/Projects/AtomicDrive/cache"),
        });

        let device_peer_id = IndexPeerId::from(PeerId::random());

        let mut device = VIndex::default();

        for event in fs.quick_full_walk() {
            match event.event_type {
                file::FileEventType::Created => {
                    let stat = fs.stat_file(event.path.clone());
                    if stat.file_type == FileType::File {
                        let data = fs.read_file(event.path.clone());
                        device.apply(device.write(
                            event.path.to_string(),
                            chunks(&data),
                            stat.last_write_time,
                            device_peer_id,
                            device.read_ctx().derive_add_ctx(device_peer_id),
                        ))
                    }
                }
                file::FileEventType::Deleted => todo!(),
                file::FileEventType::Changed => todo!(),
            }
        }

        Self {
            file_system: fs,
            device,
        }
    }
}

impl eframe::App for SyncingApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(&ctx, |ui| {
            ui.add(egui::Label::new("Hello World!"));
            ui.label("A shorter and more convenient way to add a label.");
            ui.text_edit_multiline(&mut tree_stringify(self.device.iter(), "/"));
            if ui.button("Click me").clicked() {
                // take some action here
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    // Log to stdout (if you run with `RUST_LOG=debug`).
    tracing_subscriber::fmt::init();

    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "Atomic Drive",
        native_options,
        Box::new(|cc| Box::new(SyncingApp::new())),
    )
}
